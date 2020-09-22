library(tidyverse)
library(skyscraper)
library(chariot)

conn <- chariot::connectAthena()
Tables <- pg13::lsTables(conn = conn,
                         schema = "cancergov")
if ("DRUG_DICTIONARY_LOG" %in% Tables) {
        pg13::appendTable(conn = conn,
                          schema = "cancergov",
                          tableName = "DRUG_DICTIONARY_LOG",
                          tibble::tibble(ddl_datetime = Sys.time(),
                                         drug_count = skyscraper::nci_count()))
} else {
        pg13::writeTable(conn = conn,
                         schema = "cancergov",
                         tableName = "DRUG_DICTIONARY_LOG",
                         tibble::tibble(ddl_datetime = Sys.time(),
                                        drug_count = skyscraper::nci_count()))
}

chariot::dcAthena(conn = conn)



starting_count <-
        chariot::queryAthena("SELECT COUNT(*)
                             FROM cancergov.drug_link;",
                             override_cache = TRUE) %>%
        unlist()

conn <- chariot::connectAthena()
skyscraper::get_drug_detail_links(conn = conn)
chariot::dcAthena(conn = conn,
                  remove = TRUE)


new_count <-
        chariot::queryAthena("SELECT COUNT(*)
                             FROM cancergov.drug_link;",
                             override_cache = TRUE) %>%
        unlist()

if (new_count != starting_count) {
        skyscraper::export_schema_to_data_repo("~/GitHub/cancergovData/",
                                               schema = "cancergov")
}
