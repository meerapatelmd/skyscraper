library(tidyverse)
library(chariot)
library(pg13)
library(skyscraper)

path_to_report <- paste0("~/Desktop/chemidplus_tables_to_omop_", Sys.Date(), ".txt")
conn <- chariot::connectAthena()
skyscraper::chemidplus_tables_to_omop(
                                conn = conn,
                                file_report_to = path_to_report)
chariot::dcAthena(conn,
                  remove = TRUE)
