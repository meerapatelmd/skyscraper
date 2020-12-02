#' @title
#' Search ChemiDPlus and Store Results
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}}
#'  \code{\link[dplyr]{select}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{filter}}
#'  \code{\link[tibble]{tribble}},\code{\link[tibble]{c("tibble", "tibble")}}
#'  \code{\link[police]{try_catch_error_as_null}}
#' @rdname cdp_run
#' @export
#' @importFrom pg13 query buildQuery
#' @importFrom dplyr select bind_rows filter mutate_if
#' @importFrom tibble tribble tibble

cdp_run <-
        function(conn,
                 conn_fun,
                 steps = c("log_registry_number",
                           "get_rn_url_validity",
                           "get_classification",
                           "get_names_and_synonyms",
                           "get_registry_numbers",
                           "get_links_to_resources"),
                 search_term,
                 search_type = "contains",
                 sleep_time = 5,
                verbose = TRUE,
                 render_sql = TRUE) {


                if (!missing(conn_fun)) {
                        conn <- eval(rlang::parse_expr(conn_fun))
                        on.exit(pg13::dc(conn = conn, verbose = verbose))
                }

                # conn <- chariot::connectAthena()
                # search_term <- "BI 836858"
                # type <- "contains"
                # sleep_time <- 5
                # schema <- "chemidplus"
                # export_repo <- FALSE


                if (verbose) {
                        cli::cat_line()
                        cli::cat_rule("Creating Tables")
                }

                start_cdp(conn = conn,
                          verbose = verbose,
                          render_sql = render_sql)

                if ("log_registry_number" %in% steps) {

                        if (verbose) {
                                cli::cat_line()
                                cli::cat_rule("Logging search to Registry Number Log")
                        }

                log_registry_number(conn = conn,
                                    raw_search_term = search_term,
                                    search_type = search_type,
                                    schema = "chemidplus",
                                    sleep_time = sleep_time,
                                    verbose = verbose)
                }


                rn_urls <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                            SqlRender::render(
                                            "
                                            SELECT DISTINCT rn_url
                                            FROM chemidplus.registry_number_log rnl
                                            WHERE rnl.raw_search_term IN ('@search_term');
                                            ",
                                            search_term = search_term),
                                    verbose = verbose,
                                    render_sql = render_sql) %>%
                                                unlist() %>%
                                                unname() %>%
                                                unique() %>%
                                                no_na()


                status_df <-
                        tibble::tribble(~rn_url, ~rn_url_response_status)

                for (rn_url in rn_urls) {


                        if (verbose) {
                                cli::cat_line()
                                cli::cat_rule("Scraping")
                                cli::cli_process_start(sprintf("Scraping %s", secretary::italicize(rn_url)))
                        }


                        response <- scrape_cdp(x = rn_url,
                                               sleep_time = sleep_time,
                                               verbose = verbose)


                        if (!is.null(response)) {

                                if (verbose) {
                                        cli::cli_process_done()
                                }

                                status_df <-
                                        dplyr::bind_rows(status_df,
                                                         tibble::tibble(rn_url = rn_url,
                                                                        rn_url_response_status = "Success")) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))



                                if ("get_rn_url_validity" %in% steps) {

                                        if (verbose) {
                                                cli::cat_line()
                                                cli::cat_rule("Getting RN URL Validity")
                                        }

                                get_rn_url_validity(conn = conn,
                                                    rn_url = rn_url,
                                                    response = response,
                                                    verbose = TRUE)

                                }



                                if ("get_classification" %in% steps) {

                                        if (verbose) {
                                                cli::cat_line()
                                                cli::cat_rule("Getting Classification")
                                        }

                                get_classification(conn = conn,
                                                   rn_url = rn_url,
                                                   response = response,
                                                   verbose = TRUE)

                                }




                                if ("get_names_and_synonyms" %in% steps) {

                                        if (verbose) {
                                                cli::cat_line()
                                                cli::cat_rule("Getting Names and Synonyms")
                                        }

                                get_names_and_synonyms(conn = conn,
                                                       rn_url = rn_url,
                                                       response = response,
                                                       verbose = TRUE)

                                }


                                if ("get_registry_numbers" %in% steps) {

                                        if (verbose) {
                                                cli::cat_line()
                                                cli::cat_rule("Getting Registry Numbers")
                                        }

                                get_registry_numbers(conn = conn,
                                                     rn_url = rn_url,
                                                     response = response,
                                                     verbose = TRUE)

                                }


                                if ("get_links_resources" %in% steps) {

                                        if (verbose) {
                                                cli::cat_line()
                                                cli::cat_rule("Getting Links to Resources")
                                        }


                                get_links_to_resources(conn = conn,
                                                       rn_url = rn_url,
                                                       response = response,
                                                       verbose = TRUE)

                                }

                        } else {

                                if (verbose) {
                                        cli::cli_process_failed()
                                }

                                status_df <-
                                        dplyr::bind_rows(status_df,
                                                         tibble::tibble(rn_url = rn_url,
                                                                        rn_url_response_status = "Fail")) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))
                        }

                        closeAllConnections()


                }

                status_df


        }
