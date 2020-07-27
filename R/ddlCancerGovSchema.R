#' DDL Standardized Vocabulary Tables
#' @description The ddl will only execute on the condition that there are 0 tables current in the `hemonc_extension` schema.
#' @import pg13
#' @import SqlRender
#' @export


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
