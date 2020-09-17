#' @title
#' Check if the RN URL derived from the Registry Number is valid
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
                                                                     whereInVector = rn_url))

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



