library(tidyverse)
library(skyscraper)
library(chariot)

conn <- chariot::connectAthena()
skyscraper::log_drug_count(conn = conn)
# Tables <- pg13::lsTables(conn = conn,
#                          schema = "cancergov")
#
# nci_dd_count <- skyscraper::nci_count()
#
# if ("DRUG_DICTIONARY_LOG" %in% Tables) {
#
#         drug_dictionary_log_table <-
#                 pg13::readTable(conn = conn,
#                                 schema = "cancergov",
#                                 tableName = "DRUG_DICTIONARY_LOG")
#
#
#         if (!(nci_dd_count %in% drug_dictionary_log_table$drug_count)) {
#
#                 pg13::appendTable(conn = conn,
#                                   schema = "cancergov",
#                                   tableName = "DRUG_DICTIONARY_LOG",
#                                   tibble::tibble(ddl_datetime = Sys.time(),
#                                                  drug_count = nci_dd_count))
#
#         }
#
# } else {
#
#         pg13::writeTable(conn = conn,
#                          schema = "cancergov",
#                          tableName = "DRUG_DICTIONARY_LOG",
#                          tibble::tibble(ddl_datetime = Sys.time(),
#                                         drug_count = nci_dd_count))
#
# }
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
        conn <- chariot::connectAthena()
        skyscraper::export_schema_to_data_repo(conn = conn,
                                               target_dir = "~/GitHub/cancergovData/",
                                               schema = "cancergov")
        chariot::dcAthena()
}

if (!interactive()) {
        report_file <- paste0("~/Desktop/cancergov_01_get_drug_links_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\tDrug Link Table Row Count: ", starting_count), sep = "\n", file = report_file, append = TRUE)
        cat(paste0("[", Sys.time(), "]\t Drug Link Table New Row Count: ", new_count), sep = "\n", file = report_file, append = TRUE)
}
