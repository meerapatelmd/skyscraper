library(skyscraper)


skyscraper::export_schema_to_data_repo(target_dir = "~/GitHub/cancergovData/",
                                       schema = "cancergov")


if (!interactive()) {
        report_file <- paste0("~/Desktop/cancergov_00_export_cancergov_schema_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\n"), file = report_file, append = TRUE)
}
