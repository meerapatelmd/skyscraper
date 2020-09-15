





parseChemiResponse <-
        function(conn,
                 phrase,
                 type) {

                loadCachedRN <-
                        function(phrase,
                                 type) {

                                R.cache::loadCache(key=list(phrase,
                                                            type),
                                                   dirs="skyscraper/chemidplus/rn")

                        }

                        connSchemas <-
                                pg13::lsSchema(conn = conn)

                        if (!("chemidplus" %in% connSchemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = "chemidplus")

                        }

                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = "chemidplus")


                        stopifnot("PHRASE_LOG" %in% chemiTables)

                        if ("PHRASE_RESPONSE_RESULT" %in% chemiTables) {

                                phrase_response_result <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = "chemidplus",
                                                                                     tableName = "phrase_response_result",
                                                                                     whereInField = "phrase",
                                                                                     whereInVector = phrase)) %>%
                                        dplyr::filter(type == type)

                        }

                        # Proceed if:
                        # phrase_response_result table is present: nrow(phrase_response_result) == 0
                        # phrase_response_result table was not present

                        if ("PHRASE_RESPONSE_RESULT" %in% chemiTables) {

                                if (nrow(phrase_response_result) == 0) {

                                        proceed <- TRUE

                                } else {

                                        proceed <- FALSE
                                }


                        } else {
                                proceed <- TRUE
                        }



                        if (proceed) {

                                # Getting 1 or more urls from log information
                                # urls <-
                                # pg13::query(conn = conn,
                                #             sql_statement = pg13::buildQuery(schema = "chemidplus",
                                #                                              tableName = "phrase_log",
                                #                                              whereInField = "phrase",
                                #                                              whereInVector = phrase)) %>%
                                #         dplyr::filter(type == type) %>%
                                #         dplyr::filter(response_cached == "TRUE") %>%
                                #         dplyr::select(url) %>%
                                #         dplyr::distinct() %>%
                                #         unlist()
                                #
                                # status_df <-
                                #         tibble::tibble(phrase_response_result_dt = Sys.time(),
                                #                        phrase = phrase,
                                #                        type = type) %>%
                                #         dplyr::mutate(url = urls)

                                response <-
                                        loadCachedRN(phrase = phrase,
                                                     type = type)

                                list(status_df,
                                     response)

                                # no_records_response <-
                                #         resp %>%
                                #         rvest::html_nodes("h3") %>%
                                #         rvest::html_text()


                        }




        }

