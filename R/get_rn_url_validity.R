#' @title
#' Check that the Registry Number URL is Valid
#'
#' @inherit chemidplus_scraping_functions description return
#'
#' @inheritSection chemidplus_scraping_functions RN URL Validity Table
#'
#' @inheritParams chemidplus_scraping_functions
#'
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[tibble]{tibble}}
#'
#' @rdname get_rn_url_validity
#'
#' @family chemidplus scraping
#'
#' @export
#'
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom tibble tibble
#' @importFrom magrittr %>%

get_rn_url_validity <-
    function(conn,
             rn_url,
             response,
             schema = "chemidplus",
             sleep_time = 3) {


        if (!missing(conn)) {

                connSchemas <-
                    pg13::lsSchema(conn = conn)

                if (!(schema %in% connSchemas)) {

                    pg13::createSchema(conn = conn,
                                       schema = schema)

                }

                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = schema)

                if ("RN_URL_VALIDITY" %in% chemiTables) {

                    rn_url_validity <-
                        pg13::query(conn = conn,
                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                     schema = schema,
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


            if (!missing(response)) {

                    if (!is.null(response)) {

                            status_df <-
                                tibble::tibble(rnuv_datetime = Sys.time(),
                                               rn_url = rn_url,
                                               is_404 = FALSE)


                    } else {
                        status_df <-
                            tibble::tibble(rnuv_datetime = Sys.time(),
                                           rn_url = rn_url,
                                           is_404 = is404(rn_url = rn_url))
                        Sys.sleep(sleep_time)
                    }

            } else {

                status_df <-
                    tibble::tibble(rnuv_datetime = Sys.time(),
                                   rn_url = rn_url,
                                   is_404 = is404(rn_url = rn_url))

                Sys.sleep(sleep_time)
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

                        if ("RN_URL_VALIDITY" %in% chemiTables) {

                            pg13::appendTable(conn = conn,
                                              schema = schema,
                                              tableName = "RN_URL_VALIDITY",
                                              status_df)

                        } else {

                            pg13::writeTable(conn = conn,
                                              schema = schema,
                                              tableName = "RN_URL_VALIDITY",
                                              status_df)
                        }

                } else {

                    status_df

                }
        }
    }



