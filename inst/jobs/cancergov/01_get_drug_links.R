library(tidyverse)
library(skyscraper)
library(chariot)

conn <- chariot::connectAthena()
Tables <- pg13::lsTables(conn = conn,
                         schema = "cancergov")

nci_dd_count <- skyscraper::nci_count()

if ("DRUG_DICTIONARY_LOG" %in% Tables) {

        drug_dictionary_log_table <-
                pg13::readTable(conn = conn,
                                schema = "cancergov",
                                tableName = "DRUG_DICTIONARY_LOG")


        if (!(nci_dd_count %in% drug_dictionary_log_table$drug_count)) {

                pg13::appendTable(conn = conn,
                                  schema = "cancergov",
                                  tableName = "DRUG_DICTIONARY_LOG",
                                  tibble::tibble(ddl_datetime = Sys.time(),
                                                 drug_count = nci_dd_count))

        }

} else {

        pg13::writeTable(conn = conn,
                         schema = "cancergov",
                         tableName = "DRUG_DICTIONARY_LOG",
                         tibble::tibble(ddl_datetime = Sys.time(),
                                        drug_count = nci_dd_count))

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

if (!interactive()) {
        report_file <- paste0("~/Desktop/cancergov_01_get_drug_links_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\tStarting Count: ", starting_count), file = report_file, append = TRUE)
        cat(paste0("[", Sys.time(), "]\tNew Count: ", new_count), file = report_file, append = TRUE)
}
