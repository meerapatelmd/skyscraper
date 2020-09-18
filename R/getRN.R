#' @title
#' Cache a ChemiDPlus API Call Response
#'
#' @description
#' Cache the response to an API call to "https://chem.nlm.nih.gov/chemidplus/name/", type, "/",  phrase" if it already has not been done so or if it has been, but the response returned NULL to retry querying. If a connection to a Postgres database is provided, the timestamp, phrase, type, url, whether a response was received at the time of the timestamp, and if the response is cached. Response Received field is NA if a cached object with the url as the key. If a connection to a Postgres database is provided, the dataframe is written to a `PHRASE_LOG` table in a `chemidplus` schema.
#'
#' @param conn          (optional) Connection to a Postgres Database.
#' @param phrase        phrase to search
#' @param type          type of search to conduct; Default: 'contains'
#' @param sleep_secs    Applicable to loops. If a response cached returns NULL, system sleep time in seconds after the url is read. PARAM_DESCRIPTION, Default: 0
#'
#' @return
#' If a connection is not provided, a dataframe of 1 row with the timestamp, phrase, type, url, whether a response was received at the time of the timestamp, and if the response is cached.
#'
#' @seealso
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[dplyr]{mutate}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#' @rdname cacheChemiResponse
#' @export
#' @importFrom tibble tibble
#' @importFrom dplyr mutate
#' @importFrom stringr str_remove_all
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables appendTable writeTable
#' @importFrom magrittr %>%

getRN <-
    function(conn,
             input,
             type = "contains",
             sleep_secs = 3) {


        .Deprecated("log_registry_number")

        # tositumomab and I 131 tositumomab
        # tositumomab and iodine-131 tositumomab
        # trametinib dimethyl sulfoxide
        # trastuzumab and hyaluronidase-oysk
        # trastuzumab emtansine
        # tretinoin
        # trifluridine and tipiracil
        # trifluridine/tipiracil
        # tucidinostat
        # uramustine
        # valaciclovir
        # valaciclovir Hcl
        # valproic acid
        # vinblastine sulfate

        #conn <- chariot::connectAthena()
        #input <- "vinblastine sulfate"
        #input <- "Anthracycline"

        if (!missing(conn)) {

                connSchemas <-
                    pg13::lsSchema(conn = conn)

                if (!("chemidplus" %in% connSchemas)) {

                    pg13::createSchema(conn = conn,
                                       schema = "chemidplus")

                }

                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = "chemidplus")

                if ("PHRASE_LOG" %in% chemiTables) {

                    phrase_log <-
                        pg13::query(conn = conn,
                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                     schema = "chemidplus",
                                                                     tableName = "phrase_log",
                                                                     whereInField = "input",
                                                                     whereInVector = input)) %>%
                        dplyr::filter(type == type) %>%
                        dplyr::filter(response_received == "TRUE")

                }


        }


        # Proceed if:
        # Connection was provided and a phrase_log table is present: nrow(phrase_log) == 0
        # Connection was provided and phrase_log table was not present
        # Connection was not provided
        if (!missing(conn)) {

            if ("PHRASE_LOG" %in% chemiTables) {

                        proceed <- nrow(phrase_log) == 0

            } else {

                        proceed <- TRUE

            }

        } else {

            proceed <- TRUE

        }

        if (proceed) {


                #Remove all spaces
                phrase <- stringr::str_remove_all(input, "\\s")


                #url <- "https://chem.nlm.nih.gov/chemidplus/name/contains/technetiumTc99m-labeledtilmanocept"
                url <- paste0("https://chem.nlm.nih.gov/chemidplus/name/", type, "/",  phrase)


                status_df <-
                    tibble::tibble(phrase_datetime = Sys.time(),
                                   input = input,
                                   phrase = phrase,
                                   type = type) %>%
                    dplyr::mutate(url = url)



                resp <- xml2::read_html(url)

                Sys.sleep(sleep_secs)

                if (!is.null(resp)) {

                                status_df <-
                                        status_df %>%
                                        dplyr::mutate(response_received = "TRUE") %>%
                                        dplyr::mutate(no_record = isNoRecord(response = resp))


                                if (!status_df$no_record) {

                                        results <-
                                            dplyr::bind_rows(
                                                    isSingleHit(response = resp),
                                                    isMultipleHits(response = resp)) %>%
                                            dplyr::mutate(url = url)

                                        status_df <-
                                            status_df %>%
                                            dplyr::mutate(response_recorded = "TRUE") %>%
                                            dplyr::left_join(results, by = "url") %>%
                                            dplyr::distinct() %>%
                                            dplyr::filter(rn_url != "https://chem.nlm.nih.gov/chemidplus/rn/NA")


                                } else {

                                    status_df <-
                                        status_df %>%
                                        dplyr::mutate(response_recorded = "FALSE") %>%
                                        dplyr::mutate(compound_match = "NA",
                                                      rn = "NA",
                                                      rn_url = "NA")


                                }


                        } else {

                                status_df <-
                                    status_df %>%
                                    dplyr::mutate(response_received = "FALSE",
                                                  no_record = "NA",
                                                  response_recorded = "FALSE")  %>%
                                    dplyr::mutate(compound_match = "NA",
                                                  rn = "NA",
                                                  rn_url = "NA")

                        }


                if (nrow(showConnections())) {
                    closeAllConnections()
                }



                if (!missing(conn)) {

                        connSchemas <-
                            pg13::lsSchema(conn = conn)

                        if (!("chemidplus" %in% connSchemas)) {

                            pg13::createSchema(conn = conn,
                                               schema = "chemidplus")

                        }


                        chemiTables <-
                            pg13::lsTables(conn = conn,
                                           schema = "chemidplus")

                        if ("PHRASE_LOG" %in% chemiTables) {

                            pg13::appendTable(conn = conn,
                                              schema = "chemidplus",
                                              tableName = "PHRASE_LOG",
                                              status_df)

                        } else {

                            pg13::writeTable(conn = conn,
                                              schema = "chemidplus",
                                              tableName = "PHRASE_LOG",
                                              status_df)
                        }

                } else {

                    status_df

                }
        }
    }



