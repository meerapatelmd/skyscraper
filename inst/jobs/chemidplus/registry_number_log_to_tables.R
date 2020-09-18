library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(police)


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

rn_urls <- sample(rn_urls)


if (!interactive()) {
        report_filename <- paste0("~/Desktop/registry_number_log_to_tables_", as.character(Sys.Date()), ".txt")
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
