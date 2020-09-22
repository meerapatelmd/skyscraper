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
