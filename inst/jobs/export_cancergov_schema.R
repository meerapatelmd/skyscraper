library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(rubix)
library(glitter)

outputPath <- "~/GitHub/cancergovData/data-raw/"

conn <- chariot::connectAthena()
cgTables <-
        pg13::lsTables(conn = conn,
                 schema = "cancergov")
cgData <-
cgTables %>%
        rubix::map_names_set(~pg13::readTable(conn = conn,
                                              schema = "cancergov",
                                              tableName = .))

chariot::dcAthena(conn = conn)


outputPaths <- paste0(outputPath, names(cgData), ".csv")

cgData %>%
        purrr::map2(outputPaths,
                    function(x,y) broca::simply_write_csv(x = x,
                                                          file = y))


loadLines <-
cgTables %>%
        purrr::map2(basename(outputPaths),
                    function(x, y) paste0(x, " <- broca::simply_read_csv('data-raw/", y, "')")) %>%
        unlist()

usethisLines <-
        paste0("usethis::use_data(", paste(cgTables, collapse = ","), ", overwrite = TRUE)")

readr::write_lines(c("library(broca)",
                     loadLines,
                     usethisLines),
                   path = paste0(outputPath, "DATASET.R"))

setwd("~/GitHub/cancergovData/")
source("data-raw/DATASET.R",
       local = TRUE)
glitter::docPushInstall(commit_message = "automated data refresh", has_vignettes = FALSE)

