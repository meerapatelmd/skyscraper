library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)


conn <- chariot::connectAthena()

phrase_log <-
        pg13::readTable(conn = conn,
                        schema = "chemidplus",
                        tableName = "phrase_log") %>%
        as_tibble() %>%
        mutate_all(as.character) %>%
        rubix::normalize_all_to_na()

rn_urls <-
        phrase_log %>%
        dplyr::filter(!is.na(rn_url)) %>%
        dplyr::select(rn_url) %>%
        dplyr::distinct() %>%
        unlist()


errors <- vector()
total <- length(rn_urls)
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

        secretary::typewrite(secretary::italicize(signif(100*((total-length(rn_urls))/total), digits = 2), "percent completed."))
        secretary::typewrite(secretary::cyanTxt(length(rn_urls), "out of", total, "to go."))
        secretary::typewrite(secretary::redTxt(length(errors), "errors."))
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

        secretary::typewrite(secretary::italicize(signif(100*((total-length(rn_urls))/total), digits = 2), "percent completed."))
        secretary::typewrite(secretary::cyanTxt(length(rn_urls), "out of", total, "to go."))
        secretary::typewrite(secretary::redTxt(length(errors), "errors."))
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

        secretary::typewrite(secretary::italicize(signif(100*((total-length(rn_urls))/total), digits = 2), "percent completed."))
        secretary::typewrite(secretary::cyanTxt(length(rn_urls), "out of", total, "to go."))
        secretary::typewrite(secretary::redTxt(length(errors), "errors."))
}

chariot::dcAthena(conn = conn)
