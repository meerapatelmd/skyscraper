library(skyscraper)
library(chariot)
library(tidyverse)

conn <- chariot::connectAthena()
skyscraper::export_schema_to_data_repo(target_dir = "~/GitHub/cancergovData/",
                           schema = "cancergov", conn = conn)
chariot::dcAthena(conn = conn,
                  remove = TRUE)

if (!interactive()) {
        report_file <- paste0("~/Desktop/cancergov_00_export_cancergov_schema_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\n"), file = report_file, append = TRUE)
}


pg13::renameTable(conn = conn,
                  schema = "cancergov",
                  tableName = "drug_link_synonym",
                  newTableName = "saved")



Sys.sleep(10)


conn <- chariot::connectAthena()
skyscraper::get_dictionary_and_links(conn = conn)
chariot::dcAthena(conn = conn)



Sys.sleep(10)


conn <- chariot::connectAthena()
skyscraper::process_drug_link_synonym(conn = conn)
chariot::dcAthena(conn = conn)


Sys.sleep(10)


conn <- chariot::connectAthena()
skyscraper::process_drug_link_url(conn = conn)
chariot::dcAthena(conn = conn)


