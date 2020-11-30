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
                 search_term,
                 type = "contains",
                 sleep_time = 5,
                 schema = "chemidplus") {

                # conn <- chariot::connectAthena()
                # search_term <- "BI 836858"
                # type <- "contains"
                # sleep_time <- 5
                # schema <- "chemidplus"
                # export_repo <- FALSE

                log_registry_number(conn = conn,
                                    raw_concept = search_term,
                                    type = type,
                                    schema = schema,
                                    sleep_time = sleep_time)
                #
                #                 log_registry_number(raw_concept = search_term,
                #                                     type = type,
                #                                     schema = schema)

                registry_number_log <-
                        pg13::query(conn = conn,
                                    sql_statement = pg13::buildQuery(fields = "rn_url",
                                                                     distinct = TRUE,
                                                                     schema = schema,
                                                                     tableName = "registry_number_log",
                                                                     whereInField = "raw_concept",
                                                                     whereInVector = search_term))

                rn_urls <-
                        registry_number_log %>%
                        dplyr::select(rn_url) %>%
                        unlist() %>%
                        unname() %>%
                        unique() %>%
                        no_na()


                status_df <-
                        tibble::tribble(~rn_url, ~rn_url_response_status)

                for (rn_url in rn_urls) {

                        response <- scrape_cdp(x = rn_url,
                                               sleep_time = sleep_time)


                        if (!is.null(response)) {

                                status_df <-
                                        dplyr::bind_rows(status_df,
                                                         tibble::tibble(rn_url = rn_url,
                                                                        rn_url_response_status = "Success")) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))

                                get_rn_url_validity(conn = conn,
                                                    rn_url = rn_url,
                                                    response = response,
                                                    schema = schema)

                                get_classification(conn = conn,
                                                   rn_url = rn_url,
                                                   response = response,
                                                   schema = schema)


                                get_names_and_synonyms(conn = conn,
                                                       rn_url = rn_url,
                                                       response = response,
                                                       schema = schema)

                                get_registry_numbers(conn = conn,
                                                     rn_url = rn_url,
                                                     response = response,
                                                     schema = schema)

                                get_links_to_resources(conn = conn,
                                                       rn_url = rn_url,
                                                       response = response,
                                                       schema = schema)

                        } else {
                                status_df <-
                                        dplyr::bind_rows(status_df,
                                                         tibble::tibble(rn_url = rn_url,
                                                                        rn_url_response_status = "Fail")) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))
                        }

                }


                closeAllConnections()

                status_df


        }
