library(tidyverse)
library(skyscraper)
library(chariot)


starting_count <-
        chariot::queryAthena("SELECT COUNT(*)
                             FROM cancergov.drug_dictionary;",
                             override_cache = TRUE) %>%
        unlist()

conn <- chariot::connectAthena()
skyscraper::get_drug_dictionary(conn = conn,
                                max_page = 50)
chariot::dcAthena(conn = conn,
                  remove = TRUE)

new_count <-
        chariot::queryAthena("SELECT COUNT(*)
                             FROM cancergov.drug_dictionary;",
                             override_cache = TRUE) %>%
        unlist()

if (new_count != starting_count) {
        conn <- chariot::connectAthena()
        skyscraper::export_schema_to_data_repo(conn = conn,
                                               target_dir = "~/GitHub/cancergovData/",
                                               schema = "cancergov")
        chariot::dcAthena()
}

if (!interactive()) {
        report_file <- paste0("~/Desktop/cancergov_03_get_drug_dictionary_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\tDrug Dictionary Row Count: ", starting_count), sep = "\n", file = report_file, append = TRUE)
        cat(paste0("[", Sys.time(), "]\tDrug Dictionary New Row Count: ", new_count), sep = "\n", file = report_file, append = TRUE)
}
