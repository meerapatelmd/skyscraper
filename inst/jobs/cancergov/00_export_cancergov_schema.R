library(skyscraper)
library(chariot)

conn <- chariot::connectAthena()
export_schema_to_data_repo(target_dir = "~/GitHub/cancergovData/",
                                       schema = "cancergov", conn = conn)
chariot::dcAthena(conn = conn,
                  remove = TRUE)

if (!interactive()) {
        report_file <- paste0("~/Desktop/cancergov_00_export_cancergov_schema_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\n"), file = report_file, append = TRUE)
}
