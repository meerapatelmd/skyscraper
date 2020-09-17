library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)


concepts <- chariot::queryAthena("SELECT DISTINCT cs.concept_synonym_name, rnl.*
                                 FROM public.concept c
                                 LEFT JOIN public.concept_synonym cs
                                 ON cs.concept_id = c.concept_id
                                 LEFT JOIN chemidplus.registry_number_log rnl
                                 ON rnl.raw_concept = cs.concept_synonym_name
                                 WHERE
                                        c.vocabulary_id = 'HemOnc'
                                                AND c.invalid_reason IS NULL
                                                AND c.domain_id = 'Drug';",
                                 override_cache = TRUE) %>%
        dplyr::filter_at(vars(!concept_synonym_name),
                         all_vars(is.na(.))) %>%
        dplyr::select(concept_synonym_name) %>%
        unlist()

#### Function
runHemOncToRNL <-
        function(concepts,
                 sleep_time) {
                error_concepts <- vector()
                total_concepts <- length(concepts)

                while (length(concepts)) {

                        concept <- concepts[1]

                        conn <- chariot::connectAthena()

                        output <-
                                tryCatch(
                                        log_registry_number(conn = conn,
                                                            raw_concept = concept,
                                                            sleep_time = sleep_time),
                                        error = function(e) paste("Error")
                                )

                        chariot::dcAthena(conn = conn,
                                          remove = TRUE)


                        if (length(output)) {

                                if (output == "Error") {

                                        error_concepts <-
                                                c(error_concepts,
                                                  concept)

                                }
                        }

                        concepts <- concepts[-1]
                        rm(output)

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
                return(error_concepts)
        }


if (!interactive()) {
        report_filename <- paste0("~/Desktop/hemonc_to_registry_number_log_", as.character(Sys.Date()), ".txt")
        cat(file = report_filename)
}


if (!interactive()) {
        cat("########### First Iteration\n", file = report_filename, append = TRUE)
}

error_concepts <-
runHemOncToRNL(concepts = concepts,
               sleep_time = 5)


if (!interactive()) {
        cat("########### Second Iteration\n", file = report_filename, append = TRUE)
}

concepts <- error_concepts
rm(error_concepts)

runHemOncToRNL(concepts = concepts,
               sleep_time = 10)


if (!interactive()) {
        cat("########### Third Iteration\n", file = report_filename, append = TRUE)
}

concepts <- error_concepts
rm(error_concepts)

runHemOncToRNL(concepts = concepts,
               sleep_time = 20)


if (!interactive()) {
        cat("########### COMPLETE\n", file = report_filename, append = TRUE)

        cat("\n########### ERRORS\n", file = report_filename, append = TRUE)
        cat(error_concepts, sep = "\n", file = report_filename, append = TRUE)

} else {
        secretary::typewrite_bold("ERRORS:")
        error_concepts %>%
                purrr::map(~secretary::typewrite(., tabs = 1))
}

