library(tidyverse)
conn <- chariot::connectAthena()
pg13::dropSchema(conn = conn,
                 schema = "chemidplus",
                 cascade = TRUE)

pg13::createSchema(conn = conn,
                   schema = "chemidplus")

concepts <- chariot::queryAthena("SELECT DISTINCT concept_name FROM public.concept WHERE vocabulary_id = 'HemOnc' AND invalid_reason IS NULL AND domain_id = 'Drug';")
concepts <- unlist(concepts)

error_concepts <- vector()
total_concepts <- length(concepts)
while (length(concepts)) {
        concept <- concepts[1]

        output <-
                tryCatch(
                        getRN(conn = conn,
                              input = concept,
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
        secretary::typewrite(secretary::redTxt(signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), "percent completed."))
        secretary::typewrite(secretary::redTxt(length(concepts), "out of", total_concepts, "to go."))
        secretary::typewrite(secretary::redTxt(length(error_concepts), "errors."))
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


