library(tidyverse)
conn <- chariot::connectAthena()
pg13::dropSchema(conn = conn,
                 schema = "chemidplus",
                 cascade = TRUE)

pg13::createSchema(conn = conn,
                   schema = "chemidplus")

concepts <- chariot::queryAthena("SELECT DISTINCT concept_name FROM cancergov.concept")
concepts <- unlist(concepts)


pg13::dropTable(conn = conn,
                schema = "chemidplus",
                tableName = "concept_log")

pg13::send(conn = conn,
           sql_statement =
           "CREATE TABLE chemidplus.concept_log (
                        log_datetime varchar(25) NOT NULL,
                        phrase TEXT NOT NULL,
                        type varchar(25) NOT NULL,
                        url TEXT NOT NULL,
                        url_connection_status varchar(25) NOT NULL,
                        response_status varchar(25) NOT NULL,
                        response_type varchar(25) NOT NULL,
                        compound_match TEXT NOT NULL,
                        response TEXT NOT NULL
           )")

while (length(concepts)) {
        concept <- concepts[1]

        output <-
                police::try_catch_error_as_null(
                scrapeChemiDPlus(phrase = concept) %>%
                dplyr::mutate(LOG_DATETIME = as.character(Sys.time())) %>%
                dplyr::select(LOG_DATETIME,
                              everything())
                )

        if (!is.null(output)) {

                pg13::appendTable(conn = conn,
                                  schema = "chemidplus",
                                  tableName = "concept_log",
                                  output)
        }

        #secretary::press_enter()
        concepts <- concepts[-1]
}
