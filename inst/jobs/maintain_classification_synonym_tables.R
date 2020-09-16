library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)


conn <- chariot::connectAthena()


rn_urls <-
chariot::queryAthena("SELECT pl.*, s.scrape_datetime, s.concept_synonym_name
                     FROM chemidplus.phrase_log pl
                     LEFT JOIN chemidplus.synonyms s
                     ON s.rn_url = pl.rn_url
                     WHERE pl.rn_url <> 'NA';") %>%
        dplyr::filter_at(vars(c(scrape_datetime,
                                concept_synonym_name)),
                         all_vars(is.na(.))) %>%
        dplyr::select(rn_url) %>%
        dplyr::distinct() %>%
        unlist() %>%
        unname()

if (!interactive()) {
        report_filename <- paste0("~/Desktop/maintain_classification_synonym_tables_", as.character(Sys.Date()), ".txt")
        cat(file = report_filename)
}


errors <- vector()
total <- length(rn_urls)


if (!interactive()) {
        cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
        cat("### First Iteration\n", file = report_filename, append = TRUE)
}


while (length(rn_urls)) {
        rn_url <- rn_urls[1]

        output <-
                tryCatch(
                        skyscraper::scrapeRN(
                                 conn = conn,
                                 rn_url = rn_url,
                                 sleep_time = 5),
                        error = function(e) paste("Error")
                )


        if (length(output)) {

                if (output == "Error") {

                        errors <-
                                c(errors,
                                  rn_url)

                }
        }

        rn_urls <- rn_urls[-1]

        if (interactive()) {

                secretary::typewrite(secretary::italicize(signif(100*((total-length(rn_urls))/total), digits = 2), "percent completed."))
                secretary::typewrite(secretary::cyanTxt(length(rn_urls), "out of", total, "to go."))
                secretary::typewrite(secretary::redTxt(length(errors), "errors."))

        } else {

                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(rn_urls), "/", total, " (", signif(100*((total-length(rn_urls))/total), digits = 2), " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(errors), " errors\n", sep = "", file = report_filename, append = TRUE)

        }

}

if (!interactive()) {
        cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
        cat("### Second Iteration\n", file = report_filename, append = TRUE)
}

rn_urls <- errors
errors <- vector()
total <- length(rn_urls)
while (length(rn_urls)) {
        rn_url <- rn_urls[1]

        output <-
                tryCatch(
                        skyscraper::scrapeRN(
                                 conn = conn,
                                 rn_url = rn_url,
                                 sleep_time = 10),
                        error = function(e) paste("Error")
                )


        if (length(output)) {

                if (output == "Error") {

                        errors <-
                                c(errors,
                                  rn_url)

                }
        }

        rn_urls <- rn_urls[-1]

        if (interactive()) {

                secretary::typewrite(secretary::italicize(signif(100*((total-length(rn_urls))/total), digits = 2), "percent completed."))
                secretary::typewrite(secretary::cyanTxt(length(rn_urls), "out of", total, "to go."))
                secretary::typewrite(secretary::redTxt(length(errors), "errors."))

        } else {

                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(rn_urls), "/", total, " (", signif(100*((total-length(rn_urls))/total), digits = 2), " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(errors), " errors\n", sep = "", file = report_filename, append = TRUE)

        }

}

rn_urls <- errors
errors <- vector()
total <- length(rn_urls)

if (!interactive()) {
        cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
        cat("### Third Iteration\n", file = report_filename, append = TRUE)
}


while (length(rn_urls)) {
        rn_url <- rn_urls[1]

        output <-
                tryCatch(
                        skyscraper::scrapeRN(
                                conn = conn,
                                 rn_url = rn_url,
                                 sleep_time = 15),
                        error = function(e) paste("Error")
                )


        if (length(output)) {

                if (output == "Error") {

                        errors <-
                                c(errors,
                                  rn_url)

                }
        }

        rn_urls <- rn_urls[-1]

        if (interactive()) {

                secretary::typewrite(secretary::italicize(signif(100*((total-length(rn_urls))/total), digits = 2), "percent completed."))
                secretary::typewrite(secretary::cyanTxt(length(rn_urls), "out of", total, "to go."))
                secretary::typewrite(secretary::redTxt(length(errors), "errors."))

        } else {

                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(rn_urls), "/", total, " (", signif(100*((total-length(rn_urls))/total), digits = 2), " percent completed)\n", sep = "", file = report_filename, append = TRUE)
                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", length(errors), " errors\n", sep = "", file = report_filename, append = TRUE)

        }
}

if (!interactive()) {
        cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
        cat("### Complete\n", file = report_filename, append = TRUE)

        cat("### ERRORS\n", file = report_filename, append = TRUE)
        cat(errors, sep = "\n", file = report_filename, append = TRUE)
}


chariot::dcAthena(conn = conn)
