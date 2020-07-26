#' Create CancerGov Schema
#' @description This function creates the "cancergov" schema if it doesn't exist
#' @import pg13
#' @export




createCancerGovSchema <-
            function(conn) {

                pg13::send(conn = conn,
                           sql_statement = pg13::renderCreateSchema(schema = "cancergov"))


            }
