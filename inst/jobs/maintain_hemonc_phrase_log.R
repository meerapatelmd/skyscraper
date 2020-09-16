library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)

conn <- chariot::connectAthena()

concepts <- chariot::queryAthena("SELECT DISTINCT cs.concept_synonym_name, pl.*
                                 FROM public.concept c
                                 LEFT JOIN public.concept_synonym cs
                                 ON cs.concept_id = c.concept_id
                                 LEFT JOIN chemidplus.phrase_log pl
                                 ON pl.input = cs.concept_synonym_name
                                 WHERE
                                        c.vocabulary_id = 'HemOnc'
                                                AND c.invalid_reason IS NULL
                                                AND c.domain_id = 'Drug';") %>%
                dplyr::filter_at(vars(!concept_synonym_name),
                                 all_vars(is.na(.))) %>%
                dplyr::select(concept_synonym_name) %>%
                unlist()



if (!interactive()) {
        report_filename <- paste0("~/Desktop/maintain_hemonc_phrase_log_", as.character(Sys.Date()), ".txt")
        cat(file = report_filename)
}

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

        if (interactive()) {

                secretary::typewrite(secretary::italicize(signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), "percent completed."))
                secretary::typewrite(secretary::cyanTxt(length(concepts), "out of", total_concepts, "to go."))
                secretary::typewrite(secretary::redTxt(length(error_concepts), "errors."))

        } else {

                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(concepts), "/", total_concepts, " (", signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(error_concepts), " errors\n", sep = "", file = report_filename, append = TRUE)

        }
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
                              sleep_secs = 10),
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

        if (interactive()) {

                secretary::typewrite(secretary::italicize(signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), "percent completed."))
                secretary::typewrite(secretary::cyanTxt(length(concepts), "out of", total_concepts, "to go."))
                secretary::typewrite(secretary::redTxt(length(error_concepts), "errors."))

        } else {

                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(concepts), "/", total_concepts, " (", signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(error_concepts), " errors\n", sep = "", file = report_filename, append = TRUE)

        }
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
                                          sleep_secs = 15),
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

        if (interactive()) {

                secretary::typewrite(secretary::italicize(signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), "percent completed."))
                secretary::typewrite(secretary::cyanTxt(length(concepts), "out of", total_concepts, "to go."))
                secretary::typewrite(secretary::redTxt(length(error_concepts), "errors."))

        } else {


                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(concepts), "/", total_concepts, " (", signif(100*((total_concepts-length(concepts))/total_concepts), digits = 2), " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(error_concepts), " errors\n", sep = "", file = report_filename, append = TRUE)

        }
}



chariot::dcAthena(conn = conn)
