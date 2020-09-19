library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(police)


rn_urls <-
        chariot::queryAthena(
                "SELECT DISTINCT
                        rnl.*, rnuv.rnuv_datetime, rnuv.is_404
                FROM chemidplus.rn_url_validity rnuv
                LEFT JOIN chemidplus.registry_number_log rnl
                ON rnl.rn_url = rnuv.rn_url
                WHERE rnuv.rn_url IS NOT NULL AND rnuv.is_404 = 'TRUE';",
                override_cache = TRUE) %>%
        dplyr::distinct()


URL <- "https://chem.nlm.nih.gov/chemidplus/name/contains/Abacin"
response <- xml2::read_html(x = URL)
chem_name <-
response %>%
        rvest::html_nodes(".chem-name") %>%
        rvest::html_text()


rn_urls <- sample(rn_urls)


if (!interactive()) {
        report_filename <- paste0("~/Desktop/registry_number_log_to_tables_", as.character(Sys.Date()), ".txt")
        cat(file = report_filename)
}


if (length(rn_urls)) {
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

                        Sys.sleep(5)

                        if (is.null(response)) {


                                response <-  police::try_catch_error_as_null(xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE")))

                                Sys.sleep(5)

                        }


                        if (!is.null(response)) {

                                conn <- chariot::connectAthena()
                                output <-
                                        tryCatch(
                                                get_rn_url_validity(conn = conn,
                                                                    rn_url = rn_url,
                                                                    response = response),
                                                error = function(e) paste("Error")
                                                )
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



                                # conn <- chariot::connectAthena()
                                # get_classification_code(conn = conn,
                                #                                    rn_url = rn_url,
                                #                                    response = response,
                                #                                    sleep_time = 0)
                                # chariot::dcAthena(conn = conn,
                                #                   remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_classification(conn = conn,
                                                        rn_url = rn_url,
                                                        response = response,
                                                        sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_registry_numbers(conn = conn,
                                                                    rn_url = rn_url,
                                                                    response = response,
                                                                    sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_links_to_resources(conn = conn,
                                                                 rn_url = rn_url,
                                                                 response = response,
                                                                 sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                        } else {
                                conn <- chariot::connectAthena()
                                output <-
                                                get_rn_url_validity(conn = conn,
                                                                    rn_url = rn_url)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)

                                errors <-
                                        c(errors,
                                          rn_url)

                                # if (length(output)) {
                                #
                                #         if (output == "Error") {
                                #
                                #                 errors <-
                                #                         c(errors,
                                #                           rn_url)
                                #
                                #         }
                                # }


                        }




                        rn_urls <- rn_urls[-1]
                        rm(rn_url)
                        rm(response)
                        rm(output)

                        if (nrow(showConnections())) {
                                closeAllConnections()
                        }

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
                        cat("########### Second Iteration\n", file = report_filename, append = TRUE)
                }

                rn_urls <- errors
                errors <- vector()
                total <- length(rn_urls)

                while (length(rn_urls)) {
                        rn_url <- rn_urls[1]


                        response <-
                                police::try_catch_error_as_null(
                                        xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))
                                )

                        Sys.sleep(10)

                        if (is.null(response)) {


                                response <-  police::try_catch_error_as_null(xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE")))

                                Sys.sleep(10)

                        }


                        if (!is.null(response)) {

                                conn <- chariot::connectAthena()
                                output <-
                                        tryCatch(
                                                get_rn_url_validity(conn = conn,
                                                                    rn_url = rn_url,
                                                                    response = response),
                                                error = function(e) paste("Error")
                                        )
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



                                # conn <- chariot::connectAthena()
                                # get_classification_code(conn = conn,
                                #                                    rn_url = rn_url,
                                #                                    response = response,
                                #                                    sleep_time = 0)
                                # chariot::dcAthena(conn = conn,
                                #                   remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_classification(conn = conn,
                                                               rn_url = rn_url,
                                                               response = response,
                                                               sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_registry_numbers(conn = conn,
                                                                 rn_url = rn_url,
                                                                 response = response,
                                                                 sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_links_to_resources(conn = conn,
                                                                   rn_url = rn_url,
                                                                   response = response,
                                                                   sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                        } else {
                                conn <- chariot::connectAthena()
                                output <-
                                        tryCatch(
                                                get_rn_url_validity(conn = conn,
                                                                    rn_url = rn_url),
                                                error = function(e) paste("Error")
                                        )
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                                errors <-
                                        c(errors,
                                          rn_url)

                                # if (length(output)) {
                                #
                                #         if (output == "Error") {
                                #
                                #                 errors <-
                                #                         c(errors,
                                #                           rn_url)
                                #
                                #         }
                                # }


                        }




                        rn_urls <- rn_urls[-1]
                        rm(rn_url)
                        rm(response)
                        rm(output)


                        if (nrow(showConnections())) {
                                closeAllConnections()
                        }

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
                        cat("########### Third Iteration\n", file = report_filename, append = TRUE)
                }


                rn_urls <- errors
                errors <- vector()
                total <- length(rn_urls)

                while (length(rn_urls)) {
                        rn_url <- rn_urls[1]


                        response <-
                                police::try_catch_error_as_null(
                                        xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))
                                )

                        Sys.sleep(20)

                        if (is.null(response)) {



                                response <-  police::try_catch_error_as_null(xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE")))


                                Sys.sleep(20)

                        }


                        if (!is.null(response)) {

                                conn <- chariot::connectAthena()
                                output <-
                                        tryCatch(
                                                get_rn_url_validity(conn = conn,
                                                                    rn_url = rn_url,
                                                                    response = response),
                                                error = function(e) paste("Error")
                                        )
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



                                # conn <- chariot::connectAthena()
                                # get_classification_code(conn = conn,
                                #                                    rn_url = rn_url,
                                #                                    response = response,
                                #                                    sleep_time = 0)
                                # chariot::dcAthena(conn = conn,
                                #                   remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_classification(conn = conn,
                                                               rn_url = rn_url,
                                                               response = response,
                                                               sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_registry_numbers(conn = conn,
                                                                 rn_url = rn_url,
                                                                 response = response,
                                                                 sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                                conn <- chariot::connectAthena()
                                skyscraper::get_links_to_resources(conn = conn,
                                                                   rn_url = rn_url,
                                                                   response = response,
                                                                   sleep_time = 0)
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                        } else {
                                conn <- chariot::connectAthena()
                                output <-
                                        tryCatch(
                                                get_rn_url_validity(conn = conn,
                                                                    rn_url = rn_url),
                                                error = function(e) paste("Error")
                                        )
                                chariot::dcAthena(conn = conn,
                                                  remove = TRUE)


                                errors <-
                                        c(errors,
                                          rn_url)


                                # if (length(output)) {
                                #
                                #         if (output == "Error") {
                                #
                                #                 errors <-
                                #                         c(errors,
                                #                           rn_url)
                                #
                                #         }
                                # }


                        }




                        rn_urls <- rn_urls[-1]
                        rm(rn_url)
                        rm(response)
                        rm(output)

                        if (nrow(showConnections())) {
                                closeAllConnections()
                        }

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
                        cat("########### COMPLETE\n", file = report_filename, append = TRUE)

                        cat("\n########### ERRORS\n", file = report_filename, append = TRUE)
                        cat(errors, sep = "\n", file = report_filename, append = TRUE)

                } else {
                        secretary::typewrite_bold("ERRORS:")
                        errors %>%
                                purrr::map(~secretary::typewrite(., tabs = 1))
                }
} else {

        if (interactive()) {

                secretary::typewrite_italic("No new RN urls.")

        } else {
                cat("[", as.character(Sys.time()), "]", sep = "", file = report_filename, append = TRUE)
                cat("\t", "All RN urls in in the Registry Number Log are in the RN URL Validity Table. There are no new RN URLs to parse into tables.", sep = "", file = report_filename, append = TRUE)
        }



}
