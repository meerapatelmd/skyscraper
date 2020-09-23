library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)


if (!interactive()) {
        report_filename <- paste0("~/Desktop/pubmed_search_01_pubmed_search_", as.character(Sys.Date()), ".txt")
        cat(file = report_filename)
}

concepts <-
        chariot::queryAthena(
                "
                SELECT DISTINCT
                        concept_name
                FROM chemidplus_search.concept
                ;
                ",
                override_cache = TRUE
        ) %>%
        unlist() %>%
        unname()

concepts <- sample(concepts)


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
                        skyscraper::log_registry_number(conn = conn,
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

        if ((completed_ct %% 50) == 0) {
                skyscraper::export_schema_to_data_repo(target_dir = "~/GitHub/chemidplusData/",
                                                       schema = "chemidplus")
        }


        rm(current_ct,
           completed_ct,
           percent_completed,
           error_ct)
}
