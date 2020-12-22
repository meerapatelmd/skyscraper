#' @title
#' Run NCI Drug Dictionary
#' @seealso
#'  \code{\link[rlang]{parse_expr}}
#'  \code{\link[pg13]{dc}},\code{\link[pg13]{appendTable}}
#' @rdname nci_run
#' @export
#' @importFrom rlang parse_expr
#' @importFrom pg13 dc appendTable
#' @importFrom dplyr distinct

nci_run <-
        function(conn,
                 conn_fun,
                 steps = c("nci_log_count",
                           "get_nci_dd",
                           "get_ncit"),
                 crawl_delay = 5,
                 size = 10000,
                 expiration_days = 10,
                 verbose = TRUE,
                 render_sql = TRUE) {

                on.exit(expr = closeAllConnections())


                if (!missing(conn_fun)) {

                        conn <- eval(rlang::parse_expr(conn_fun))
                        on.exit(pg13::dc(conn = conn,
                                         verbose = verbose))

                }


                start_nci(conn = conn,
                          verbose = verbose,
                          render_sql = render_sql)

                if ("nci_log_count" %in% steps) {
                nci_log_count(conn = conn,
                              verbose = verbose,
                              render_sql = render_sql,
                              crawl_delay = crawl_delay)

                }


                if ("get_nci_dd" %in% steps) {

                output <- get_nci_dd(crawl_delay = crawl_delay,
                                           size = size,
                                           verbose = verbose)


                results <- output$results %>%
                                dplyr::distinct()

                pg13::appendTable(conn = conn,
                                  schema = "cancergov",
                                  tableName = "nci_drug_dictionary",
                                  data = results,
                                  verbose = verbose,
                                  render_sql = render_sql)

                }


                if ("get_ncit" %in% steps) {

                        get_ncit(conn = conn,
                                 sleep_time = crawl_delay,
                                 expiration_days = expiration_days,
                                 verbose = verbose,
                                 render_sql = render_sql)
                }




        }
