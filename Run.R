library(tidyverse)
conn <- chariot::connectAthena()
pg13::dropSchema(conn = conn,
                 schema = "chemidplus",
                 cascade = TRUE)

pg13::createSchema(conn = conn,
                   schema = "chemidplus")

concepts <- chariot::queryAthena("SELECT DISTINCT concept_name FROM public.concept WHERE vocabulary_id = 'HemOnc' AND invalid_reason IS NULL AND domain_id IN ('Drug', 'Regimen')")
concepts <- unlist(concepts)


pg13::dropTable(conn = conn,
                schema = "chemidplus",
                tableName = "url_status_log")

# pg13::send(conn = conn,
#            sql_statement =
#            "CREATE TABLE chemidplus.url_status_log (
#                         status_datetime varchar(25) NOT NULL,
#                         phrase TEXT NOT NULL,
#                         type varchar(25) NOT NULL,
#                         url TEXT NOT NULL,
#                         url_connection_status varchar(25) NOT NULL
#            )")

error_concepts <- vector()
total_concepts <- length(concepts)
while (length(concepts)) {
        concept <- concepts[1]

        output <-
                tryCatch(
                        cacheChemiResponse(conn = conn,
                                           phrase = concept,
                                           sleep_secs = 3),
                        error = function(e) paste("Error")
                )

         if (output == "Error") {

                 error_concepts <-
                         c(error_concepts,
                           concept)

         }

        #secretary::press_enter()
        concepts <- concepts[-1]
        secretary::typewrite(secretary::redTxt(signif(100*length(concepts)/total_concepts, digits = 2), "% to go."))
        secretary::typewrite(secretary::redTxt(length(concepts), "out of", total_concepts, "to go."))
}

error_concepts2 <- vector()
while (length(error_concepts)) {
        concept <- error_concepts[1]

        output <-
                police::try_catch_error_as_null(
                        chemiDPlusURLStatus(phrase = concept) %>%
                                dplyr::mutate(status_datetime = as.character(Sys.time()))
                )

        Sys.sleep(5)


        if (!is.null(output)) {

                # pg13::appendTable(conn = conn,
                #                   schema = "chemidplus",
                #                   tableName = "url_status_log",
                #                   output)
        } else {
                error_concepts2 <-
                        c(error_concepts2,
                          concept)
        }

        #closeAllConnections()

        #secretary::press_enter()
        error_concepts <- error_concepts[-1]
}


