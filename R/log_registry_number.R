#' @title
#' Log Registry Number Matches for a Search
#'
#' @inherit chemidplus_scraping_functions description return
#'
#' @inheritSection chemidplus_scraping_functions Registry Number Log Table
#'
#' @param conn          Postgres connection object
#' @param schema        Schema that the returned data is written to, Default: 'chemidplus'
#' @param sleep_time    If the response argument is missing, the number seconds to pause after reading the URL, Default: 3
#' @param raw_concept       Character string of length 1 to be searched in ChemiDPlus
#' @param type              Type of search available at \href{https://chem.nlm.nih.gov/chemidplus/}{ChemiDPlus}, DEFAULT: "contains"
#'
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{distinct}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[xml2]{read_xml}}
#'
#' @rdname log_registry_number
#' @family chemidplus
#'
#' @export
#'
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom dplyr filter mutate bind_rows left_join distinct
#' @importFrom stringr str_remove_all
#' @importFrom tibble tibble
#' @importFrom xml2 read_html
#' @importFrom magrittr %>%

log_registry_number <-
    function(conn,
             raw_concept,
             type = "contains",
             sleep_time = 3,
             schema = "chemidplus") {

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
        #raw_concept <- "Interleukin-2"
        #raw_concept <- "Anthracycline"

        #raw_concept <- "olmutinib"
        #

        search_type <- type

        if (!missing(conn)) {

                connSchemas <-
                    pg13::lsSchema(conn = conn)

                if (!(schema %in% connSchemas)) {

                    pg13::createSchema(conn = conn,
                                       schema = schema)

                }

                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = schema)

                if ("REGISTRY_NUMBER_LOG" %in% chemiTables) {

                    registry_number_log <-
                        pg13::query(conn = conn,
                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                     schema = schema,
                                                                     tableName = "REGISTRY_NUMBER_LOG",
                                                                     whereInField = "raw_concept",
                                                                     whereInVector = raw_concept)) %>%
                        dplyr::filter(type == search_type) %>%
                        dplyr::filter(response_received == "TRUE")

                }


        }


        # Proceed if:
        # Connection was provided and a registry_number_log table is present: nrow(registry_number_log) == 0
        # Connection was provided and registry_number_log table was not present
        # Connection was not provided
        if (!missing(conn)) {

            if ("REGISTRY_NUMBER_LOG" %in% chemiTables) {

                        proceed <- nrow(registry_number_log) == 0

            } else {

                        proceed <- TRUE

            }

        } else {

            proceed <- TRUE

        }

        if (proceed) {


                #Remove all spaces
                processed_concept <- stringr::str_remove_all(raw_concept, "\\s|[']{1}")


                #url <- "https://chem.nlm.nih.gov/chemidplus/name/contains/technetiumTc99m-labeledtilmanocept"
                url <- paste0("https://chem.nlm.nih.gov/chemidplus/name/", search_type, "/",  processed_concept)


                status_df <-
                    tibble::tibble(rnl_datetime = Sys.time(),
                                   raw_concept = raw_concept,
                                   processed_concept = processed_concept,
                                   type = search_type) %>%
                    dplyr::mutate(url = url)



                resp <- xml2::read_html(url)

                Sys.sleep(sleep_time)

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
                                        dplyr::mutate(compound_match = NA,
                                                      rn = NA,
                                                      rn_url = NA)


                                }


                        } else {

                                status_df <-
                                    status_df %>%
                                    dplyr::mutate(response_received = "FALSE",
                                                  no_record = NA,
                                                  response_recorded = "FALSE")  %>%
                                    dplyr::mutate(compound_match = NA,
                                                  rn = NA,
                                                  rn_url = NA)

                        }


                if (nrow(showConnections())) {
                    suppressWarnings(closeAllConnections())
                }



                if (!missing(conn)) {

                        connSchemas <-
                            pg13::lsSchema(conn = conn)

                        if (!(schema %in% connSchemas)) {

                            pg13::createSchema(conn = conn,
                                               schema = schema)

                        }


                        chemiTables <-
                            pg13::lsTables(conn = conn,
                                           schema = schema)

                        if ("REGISTRY_NUMBER_LOG" %in% chemiTables) {

                            pg13::appendTable(conn = conn,
                                              schema = schema,
                                              tableName = "REGISTRY_NUMBER_LOG",
                                              status_df)

                        } else {

                            pg13::writeTable(conn = conn,
                                              schema = schema,
                                              tableName = "REGISTRY_NUMBER_LOG",
                                              status_df)
                        }

                } else {

                    return(status_df)

                }
        }
    }



