#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details
#' This function must be run before a log entry is made using the log_drug_count function.
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{query}}
#'  \code{\link[skyscraper]{nci_count}}
#' @rdname has_new_drug_count
#' @export
#' @importFrom pg13 lsTables query


has_new_drug_count <-
        function(conn) {


                Tables <- pg13::lsTables(conn = conn,
                                         schema = "cancergov")

                nci_dd_count <- nci_count()

                if ("DRUG_DICTIONARY_LOG" %in% Tables) {

                        most_recent_drug_count <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                                "
                                                SELECT drug_count
                                                        FROM cancergov.DRUG_DICTIONARY_LOG
                                                WHERE ddl_datetime = (
                                                        SELECT MAX(ddl_datetime)
                                                        FROM cancergov.DRUG_DICTIONARY_LOG
                                                )
                                                ;
                                                "
                                        ) %>%
                                unlist() %>%
                                as.integer()


                        if (nci_dd_count != most_recent_drug_count) {

                                                TRUE

                        } else {

                                                FALSE
                        }

                } else {

                       TRUE

                }
}
