#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{readTable}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[skyscraper]{nci_count}}
#'  \code{\link[tibble]{tibble}}
#' @rdname log_drug_count
#' @family cancergov
#' @export
#' @importFrom pg13 lsTables readTable appendTable writeTable
#' @importFrom tibble tibble


log_drug_count <-
        function(conn) {


                Tables <- pg13::lsTables(conn = conn,
                                         schema = "cancergov")

                nci_dd_count <- nci_count()

                if ("DRUG_DICTIONARY_LOG" %in% Tables) {

                        drug_dictionary_log_table <-
                                pg13::readTable(conn = conn,
                                                schema = "cancergov",
                                                tableName = "DRUG_DICTIONARY_LOG")


                        if (!(nci_dd_count %in% drug_dictionary_log_table$drug_count)) {

                                pg13::appendTable(conn = conn,
                                                  schema = "cancergov",
                                                  tableName = "DRUG_DICTIONARY_LOG",
                                                  tibble::tibble(ddl_datetime = Sys.time(),
                                                                 drug_count = nci_dd_count))

                        }

                } else {

                        pg13::writeTable(conn = conn,
                                         schema = "cancergov",
                                         tableName = "DRUG_DICTIONARY_LOG",
                                         tibble::tibble(ddl_datetime = Sys.time(),
                                                        drug_count = nci_dd_count))

                }
}
