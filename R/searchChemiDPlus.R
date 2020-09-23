#' @title
#' Search ChemiDPlus and Store Results
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}}
#'  \code{\link[dplyr]{select}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{filter}}
#'  \code{\link[centipede]{no_na}}
#'  \code{\link[tibble]{tribble}},\code{\link[tibble]{c("tibble", "tibble")}}
#'  \code{\link[police]{try_catch_error_as_null}}
#' @rdname searchChemiDPlus
#' @export
#' @importFrom pg13 query buildQuery
#' @importFrom dplyr select bind_rows filter
#' @importFrom centipede no_na
#' @importFrom tibble tribble tibble
#' @importFrom police try_catch_error_as_null
#' @importFrom magrittr %>%

searchChemiDPlus <-
        function(conn,
                 search_term,
                 type = "contains",
                 sleep_time = 3,
                 schema = "chemidplus_search",
                 export_repo = TRUE,
                 target_dir = "~/GitHub/Public-Packages/chemidplusSearchData/") {

                #search_term <- "Protocol #14-068: AG-120 Tablets"

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
                                centipede::no_na()


                        status_df <-
                                tibble::tribble(~rn_url, ~rn_url_response_status)

                        for (rn_url in rn_urls) {

                                response <- police::try_catch_error_as_null(
                                                        get_response(
                                                                rn_url = rn_url,
                                                                sleep_time = 0
                                                                )
                                                )

                                if (is.null(response)) {

                                        Sys.sleep(sleep_time)

                                        response <- police::try_catch_error_as_null(
                                                                get_response(
                                                                        rn_url = rn_url,
                                                                        sleep_time = 0
                                                                        )
                                                        )

                                }


                                if (!is.null(response)) {

                                        status_df <-
                                                dplyr::bind_rows(status_df,
                                                                 tibble::tibble(rn_url = rn_url,
                                                                                rn_url_response_status = "Success"))

                                        get_rn_url_validity(#conn = conn,
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
                                                                                rn_url_response_status = "Fail"))
                                }

                        }

                        if (export_repo) {
                                diff <- status_df %>%
                                                dplyr::filter(rn_url_response_status == "Success")

                                if (nrow(diff)) {
                                        export_schema_to_data_repo(conn = conn,
                                                                   target_dir = target_dir,
                                                                   schema = schema)
                                }
                        }


                        return(status_df)


        }
