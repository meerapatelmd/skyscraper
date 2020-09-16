library(tidyverse)
library(chariot)
library(pg13)
library(skyscraper)

conn <- chariot::connectAthena()


skyscraper::maintainRNtoOMOP(conn = conn,
                             file_report_to = paste0("~/Desktop/maintain_rn_to_omop_", Sys.Date(), ".txt"))


chariot::dcAthena(conn)
