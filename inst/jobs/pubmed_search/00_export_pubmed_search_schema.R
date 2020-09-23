library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(rubix)
library(glitter)
library(sinew)


conn <- chariot::connectAthena()
skyscraper::export_schema_to_data_repo(conn = conn,
                                       target_dir = "~/GitHub/Public-Packages/pubmedSearchData/",
                                       schema = "pubmed_search")
chariot::dcAthena(conn = conn)


if (!interactive()) {
        report_file <- paste0("~/Desktop/pubmed_search_00_export_pubmed_search_schema_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\n"), file = report_file, append = TRUE)
}
