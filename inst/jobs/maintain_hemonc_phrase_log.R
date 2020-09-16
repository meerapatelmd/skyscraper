library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)


concepts <- chariot::queryAthena("SELECT DISTINCT concept_name
                                 FROM public.concept
                                 WHERE
                                        vocabulary_id = 'HemOnc'
                                                AND invalid_reason IS NULL
                                                AND domain_id = 'Drug';")

concepts <- unlist(concepts)


conn <- chariot::connectAthena()
error_concepts <- vector()
total_concepts <- length(concepts)
while (length(concepts)) {
        concept <- concepts[1]

        output <-
                tryCatch(
                        skyscraper::getRN(
                                conn = conn,
                                input = concept,
                                sleep_secs = 5),
                        error = function(e) paste("Error")
                )


        if (length(output)) {

                if (output == "Error") {

                        error_concepts <-
                                c(error_concepts,
                                  concept)

                }
        }

        concepts <- concepts[-1]

        secretary::typewrite(secretary::italicize(signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), "percent completed."))
        secretary::typewrite(secretary::cyanTxt(length(concepts), "out of", total_concepts, "to go."))
        secretary::typewrite(secretary::redTxt(length(error_concepts), "errors."))
}


concepts <- error_concepts
error_concepts <- vector()
total_concepts <- length(concepts)
while (length(concepts)) {
        concept <- concepts[1]

        output <-
                tryCatch(
                        skyscraper::getRN(conn = conn,
                              input = concept,
                              sleep_secs = 8),
                        error = function(e) paste("Error")
                )


        if (length(output)) {

                if (output == "Error") {

                        error_concepts <-
                                c(error_concepts,
                                  concept)

                }
        }

        concepts <- concepts[-1]

        secretary::typewrite(secretary::italicize(signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), "percent completed."))
        secretary::typewrite(secretary::cyanTxt(length(concepts), "out of", total_concepts, "to go."))
        secretary::typewrite(secretary::redTxt(length(error_concepts), "errors."))
}

chariot::dcAthena(conn = conn)
