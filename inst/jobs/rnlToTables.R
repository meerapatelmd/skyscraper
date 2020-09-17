library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)


# RNL to Validity Table
conn <- chariot::connectAthena()
chemiTables <-
pg13::lsTables(conn = conn,
               schema = "chemidplus")
chariot::dcAthena(conn = conn,
                  remove = TRUE)

if ("RN_URL_VALIDITY" %in% chemiTables) {
        rn_urls <-
                chariot::queryAthena(
                    "SELECT rnl.*, v.rnuv_datetime, v.is_404
                     FROM chemidplus.registry_number_log rnl
                     LEFT JOIN chemidplus.rn_url_validity v
                     ON v.rn_url = rnl.rn_url
                     WHERE rnl.rn_url IS NOT NULL;",
                                     override_cache = TRUE) %>%
                dplyr::filter_at(vars(c(rnuv_datetime,
                                        is_404)),
                                 all_vars(is.na(.))) %>%
                dplyr::select(rn_url) %>%
                dplyr::distinct() %>%
                unlist() %>%
                unname()
} else {
        rn_urls <-
                chariot::queryAthena(
                        "SELECT DISTINCT
                                rn_url
                        FROM chemidplus.registry_number_log
                        WHERE rn_url IS NOT NULL;",
                        override_cache = TRUE) %>%
                dplyr::distinct() %>%
                unlist() %>%
                unname()
}


if (!interactive()) {
        report_filename <- paste0("~/Desktop/maintain_chemidplus_tables_", as.character(Sys.Date()), ".txt")
        cat(file = report_filename)
}


errors <- vector()
total <- length(rn_urls)



if (!interactive()) {
        cat("########### First Iteration\n", file = report_filename, append = TRUE)
}

while (length(rn_urls)) {
        rn_url <- rn_urls[1]


        response <-
                police::try_catch_error_as_null(
                xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))
                )

        if (is.null(response)) {

                Sys.sleep(5)

                response <-  police::try_catch_error_as_null(xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE")))

        }


        if (!is.null(response)) {

                conn <- chariot::connectAthena()
                output <-
                tryCatch(
                        get_rn_url_validity(conn = conn,
                                            rn_url = rn_url,
                                            response = response),
                        error = function(e) paste("Error"))

                chariot::dcAthena(conn = conn,
                                  remove = TRUE)



        if (length(output)) {

                if (output == "Error") {

                        errors <-
                                c(errors,
                                  rn_url)

                }
        }

        conn <- chariot::connectAthena()
        skyscraper::get_names_and_synonyms(conn = conn,
                                           rn_url = rn_url,
                                           response = response,
                                           sleep_time = 0)
        chariot::dcAthena(conn = conn,
                          remove = TRUE)



        conn <- chariot::connectAthena()
        get_classification_code(conn = conn,
                                           rn_url = rn_url,
                                           response = response,
                                           sleep_time = 0)
        chariot::dcAthena(conn = conn,
                          remove = TRUE)


        conn <- chariot::connectAthena()
        get_classification(conn = conn,
                                rn_url = rn_url,
                                response = response,
                                sleep_time = 0)
        chariot::dcAthena(conn = conn,
                          remove = TRUE)


        conn <- chariot::connectAthena()
        get_registry_numbers(conn = conn,
                                            rn_url = rn_url,
                                            response = response,
                                            sleep_time = 0)
        chariot::dcAthena(conn = conn,
                          remove = TRUE)


        conn <- chariot::connectAthena()
        get_links_to_resources(conn = conn,
                                         rn_url = rn_url,
                                         response = response,
                                         sleep_time = 0)
        chariot::dcAthena(conn = conn,
                          remove = TRUE)

        } else {
                conn <- chariot::connectAthena()
                get_rn_url_validity(conn = conn,
                                    rn_url = rn_url)
                chariot::dcAthena(conn = conn,
                                  remove = TRUE)

        }






        rn_urls <- rn_urls[-1]
        rm(rn_url)
        rm(response)
        rm(output)

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
