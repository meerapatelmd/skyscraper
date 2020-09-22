library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(rubix)
library(glitter)
library(sinew)


skyscraper::export_schema_to_data_repo(target_dir = "~/GitHub/chemidplusData/",
                                       schema = "chemidplus")


if (!interactive()) {
        report_file <- paste0("~/Desktop/chemidplus_00_export_chemidplus_schema_", Sys.Date(), ".txt")
        cat(paste0("[", Sys.time(), "]\n"), file = report_file, append = TRUE)
}
