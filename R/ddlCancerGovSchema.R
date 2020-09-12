#' @title
#' DDL Cancer Gov Schema
#' @description
#' If the cancergov schema does not have any tables, writes empty OMOP Vocabulary Tables
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
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{send}}
#'  \code{\link[SqlRender]{render}},\code{\link[SqlRender]{readSql}}
#' @rdname ddlCancerGovSchema
#' @export
#' @importFrom pg13 lsTables send
#' @importFrom SqlRender render readSql

ddlCancerGovSchema <-
        function(conn) {

                cgTables <-
                pg13::lsTables(conn = conn,
                               schema = "cancergov")

                if (length(cgTables) == 0) {

                        base <- system.file(package = "skyscraper")
                        path <- paste0(base, "/sql/postgresqlddl.sql")

                        sql_statement <- SqlRender::render(SqlRender::readSql(path),
                                                schema = "cancergov")
                        pg13::send(conn = conn,
                                   sql_statement = sql_statement)

                }
        }
