library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)


concepts <- chariot::queryAthena("SELECT DISTINCT cs.concept_synonym_name
                                 FROM public.concept_ancestor ca
                                 INNER JOIN public.concept c
                                 ON c.concept_id = ca.descendant_concept_id
                                 LEFT JOIN public.concept_synonym cs
                                 ON cs.concept_id = c.concept_id
                                 LEFT JOIN chemidplus.registry_number_log rnl
                                 ON rnl.raw_concept = cs.concept_synonym_name
                                 WHERE ca.ancestor_concept_id = 21601386
                                 AND c.invalid_reason IS NULL
                                 AND c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
                                 AND c.concept_class_id IN ('Ingredient', 'Precise Ingredient')
                                 AND cs.language_concept_id = 4180186
                                 AND rnl_datetime IS NULL
                                 ;",
                                 override_cache = TRUE) %>%
        unlist() %>%
        unname()


concepts <- sample(concepts)

if (!interactive()) {
        report_filename <- paste0("~/Desktop/hemonc_to_registry_number_log_", as.character(Sys.Date()), ".txt")
        cat(file = report_filename)
}


if (!interactive()) {
        cat("########### First Iteration\n", file = report_filename, append = TRUE)
}

error_concepts <- vector()

total_concepts <- length(concepts)

while (length(concepts)) {

        concept <- concepts[1]

        conn <- chariot::connectAthena()

        output <-
                tryCatch(
                        log_registry_number(conn = conn,
                                            raw_concept = concept,
                                            sleep_time = 5),
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

        rm(output)

        concepts <- concepts[-1]


        # Getting metrics
        # 1. current concepts length
        # 2. completed concepts
        # 3. percent completed
        # 4. Error concepts

        # 1
        current_ct <- length(concepts)

        # 2
        completed_ct <- total_concepts-current_ct

        # 3
        percent_completed <- signif(((completed_ct/total_concepts)*100), digits = 2)

        #4.
        error_ct <- length(error_concepts)


        if (interactive()) {

                secretary::typewrite(secretary::italicize(percent_completed), "percent completed.")
                secretary::typewrite(secretary::cyanTxt(current_ct, "out of", total_concepts, "to go."))
                secretary::typewrite(secretary::redTxt(error_ct, "errors."))

        } else {

                cat(paste0("[", as.character(Sys.time()), "]"), sep = "", file = report_filename, append = TRUE)
                cat("\t", current_ct, "/", total_concepts, " (", percent_completed, " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat(paste0("[", as.character(Sys.time()), "]"), sep = "", file = report_filename, append = TRUE)
                cat("\t", error_ct, " errors\n", sep = "", file = report_filename, append = TRUE)

        }
        rm(current_ct,
           completed_ct,
           percent_completed,
           error_ct)
}



if (!interactive()) {
        cat("########### Second Iteration\n", file = report_filename, append = TRUE)
}


concepts <- error_concepts
error_concepts <- vector()
total_concepts <- length(concepts)
concepts <- sample(concepts)
while (length(concepts)) {

        concept <- concepts[1]

        conn <- chariot::connectAthena()

        output <-
                tryCatch(
                        log_registry_number(conn = conn,
                                            raw_concept = concept,
                                            sleep_time = 10),
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

        # Getting metrics
        # 1. current concepts length
        # 2. completed concepts
        # 3. percent completed
        # 4. Error concepts

        # 1
        current_ct <- length(concepts)

        # 2
        completed_ct <- total_concepts-current_ct

        # 3
        percent_completed <- signif(((completed_ct/total_concepts)*100), digits = 2)

        #4.
        error_ct <- length(error_concepts)


        if (interactive()) {

                secretary::typewrite(secretary::italicize(percent_completed), "percent completed.")
                secretary::typewrite(secretary::cyanTxt(current_ct, "out of", total_concepts, "to go."))
                secretary::typewrite(secretary::redTxt(error_ct, "errors."))

        } else {

                cat(paste0("[", as.character(Sys.time()), "]"), sep = "", file = report_filename, append = TRUE)
                cat("\t", current_ct, "/", total_concepts, " (", percent_completed, " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat(paste0("[", as.character(Sys.time()), "]"), sep = "", file = report_filename, append = TRUE)
                cat("\t", error_ct, " errors\n", sep = "", file = report_filename, append = TRUE)

        }
        rm(current_ct,
           completed_ct,
           percent_completed,
           error_ct)
}


if (!interactive()) {
        cat("########### Third Iteration\n", file = report_filename, append = TRUE)
}

concepts <- error_concepts
error_concepts <- vector()
total_concepts <- length(concepts)
concepts <- sample(concepts)
while (length(concepts)) {

        concept <- concepts[1]

        conn <- chariot::connectAthena()

        output <-
                tryCatch(
                        log_registry_number(conn = conn,
                                            raw_concept = concept,
                                            sleep_time = 20),
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

        # Getting metrics
        # 1. current concepts length
        # 2. completed concepts
        # 3. percent completed
        # 4. Error concepts

        # 1
        current_ct <- length(concepts)

        # 2
        completed_ct <- total_concepts-current_ct

        # 3
        percent_completed <- signif(((completed_ct/total_concepts)*100), digits = 2)

        #4.
        error_ct <- length(error_concepts)


        if (interactive()) {

                secretary::typewrite(secretary::italicize(percent_completed), "percent completed.")
                secretary::typewrite(secretary::cyanTxt(current_ct, "out of", total_concepts, "to go."))
                secretary::typewrite(secretary::redTxt(error_ct, "errors."))

        } else {

                cat(paste0("[", as.character(Sys.time()), "]"), sep = "", file = report_filename, append = TRUE)
                cat("\t", current_ct, "/", total_concepts, " (", percent_completed, " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat(paste0("[", as.character(Sys.time()), "]"), sep = "", file = report_filename, append = TRUE)
                cat("\t", error_ct, " errors\n", sep = "", file = report_filename, append = TRUE)

        }
        rm(current_ct,
           completed_ct,
           percent_completed,
           error_ct)
}

if (!interactive()) {
        cat("########### Fourth Iteration\n", file = report_filename, append = TRUE)
}

concepts <- error_concepts
error_concepts <- vector()
total_concepts <- length(concepts)
concepts <- sample(concepts)
while (length(concepts)) {

        concept <- concepts[1]

        conn <- chariot::connectAthena()

        output <-
                tryCatch(
                        log_registry_number(conn = conn,
                                            raw_concept = concept,
                                            sleep_time = 30),
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

        # Getting metrics
        # 1. current concepts length
        # 2. completed concepts
        # 3. percent completed
        # 4. Error concepts

        # 1
        current_ct <- length(concepts)

        # 2
        completed_ct <- total_concepts-current_ct

        # 3
        percent_completed <- signif(((completed_ct/total_concepts)*100), digits = 2)

        #4.
        error_ct <- length(error_concepts)


        if (interactive()) {

                secretary::typewrite(secretary::italicize(percent_completed), "percent completed.")
                secretary::typewrite(secretary::cyanTxt(current_ct, "out of", total_concepts, "to go."))
                secretary::typewrite(secretary::redTxt(error_ct, "errors."))

        } else {

                cat(paste0("[", as.character(Sys.time()), "]"), sep = "", file = report_filename, append = TRUE)
                cat("\t", current_ct, "/", total_concepts, " (", percent_completed, " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat(paste0("[", as.character(Sys.time()), "]"), sep = "", file = report_filename, append = TRUE)
                cat("\t", error_ct, " errors\n", sep = "", file = report_filename, append = TRUE)

        }
        rm(current_ct,
           completed_ct,
           percent_completed,
           error_ct)
}

if (!interactive()) {
        cat("########### COMPLETE\n", file = report_filename, append = TRUE)

        cat("\n########### ERRORS\n", file = report_filename, append = TRUE)
        cat(error_concepts, sep = "\n", file = report_filename, append = TRUE)

} else {
        secretary::typewrite_bold("ERRORS:")
        error_concepts %>%
                purrr::map(~secretary::typewrite(., tabs = 1))
}

