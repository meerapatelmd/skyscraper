library(tidyverse)
library(skyscraper)
library(chariot)


starting_count <-
        chariot::queryAthena("SELECT COUNT(*)
                             FROM cancergov.drug_link_synonym;",
                             override_cache = TRUE) %>%
        unlist()

conn <- chariot::connectAthena()
skyscraper::get_drug_link_synonym(conn = conn)
chariot::dcAthena(conn = conn,
                  remove = TRUE)

new_count <-
        chariot::queryAthena("SELECT COUNT(*)
                             FROM cancergov.drug_link_synonym;",
                             override_cache = TRUE) %>%
        unlist()


if (new_count != starting_count) {
        skyscraper::export_schema_to_data_repo("~/GitHub/cancergovData/",
                                               schema = "cancergov")
}

if (!interactive()) {
        report_file <- paste0("~/Desktop/cancergov_02_get_drug_link_synonyms_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\tStarting Count: ", starting_count), sep = "\n", file = report_file, append = TRUE)
        cat(paste0("[", Sys.time(), "]\tNew Count: ", new_count), sep = "\n", file = report_file, append = TRUE)
}

