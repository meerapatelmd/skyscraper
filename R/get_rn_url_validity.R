#' @title
#' Check if the RN URL derived from the Registry Number is valid
#' @description
#' Cache the response to an API call to "https://chem.nlm.nih.gov/chemidplus/name/", type, "/",  processed_concept" if it already has not been done so or if it has been, but the response returned NULL to retry querying. If a connection to a Postgres database is provided, the timestamp, processed_concept, type, url, whether a response was received at the time of the timestamp, and if the response is cached. Response Received field is NA if a cached object with the url as the key. If a connection to a Postgres database is provided, the dataframe is written to a `PHRASE_LOG` table in a `chemidplus` schema.
#'
#' @param conn          (optional) Connection to a Postgres Database.
#' @param raw_concept   Raw concept to search
#' @param type          type of search to conduct; Default: 'contains'
#' @param sleep_time    Applicable to loops. If a response cached returns NULL, system sleep time in seconds after the url is read. PARAM_DESCRIPTION, Default: 0
#'
#' @return
#' If a connection is not provided, a dataframe of 1 row with the timestamp, processed_concept, type, url, whether a response was received at the time of the timestamp, and if the response is cached.
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

get_rn_url_validity <-
    function(conn,
             rn_url,
             sleep_time = 3) {


        if (!missing(conn)) {

                connSchemas <-
                    pg13::lsSchema(conn = conn)

                if (!("chemidplus" %in% connSchemas)) {

                    pg13::createSchema(conn = conn,
                                       schema = "chemidplus")

                }

                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = "chemidplus")

                if ("RN_URL_VALIDITY" %in% chemiTables) {

                    rn_url_validity <-
                        pg13::query(conn = conn,
                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                     schema = "chemidplus",
                                                                     tableName = "RN_URL_VALIDITY",
                                                                     whereInField = "rn_url",
                                                                     whereInVector = rn_url)) %>%
                        dplyr::filter(type == type) %>%
                        dplyr::filter(response_received == "TRUE")

                }

        }


        # Proceed if:
        # Connection was provided and a rn_url_validity table is present: nrow(rn_url_validity) == 0
        # Connection was provided and rn_url_validity table was not present
        # Connection was not provided
        if (!missing(conn)) {

            if ("RN_URL_VALIDITY" %in% chemiTables) {

                        proceed <- nrow(rn_url_validity) == 0

            } else {

                        proceed <- TRUE

            }

        } else {

            proceed <- TRUE

        }

        if (proceed) {



                status_df <-
                    tibble::tibble(rnuv_datetime = Sys.time(),
                                   rn_url = rn_url,
                                   is_404 = is404(rn_url = rn_url))

                Sys.sleep(sleep_time)

                if (nrow(showConnections())) {
                    suppressWarnings(closeAllConnections())
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

                        if ("RN_URL_VALIDITY" %in% chemiTables) {

                            pg13::appendTable(conn = conn,
                                              schema = "chemidplus",
                                              tableName = "RN_URL_VALIDITY",
                                              status_df)

                        } else {

                            pg13::writeTable(conn = conn,
                                              schema = "chemidplus",
                                              tableName = "RN_URL_VALIDITY",
                                              status_df)
                        }

                } else {

                    status_df

                }
        }
    }



