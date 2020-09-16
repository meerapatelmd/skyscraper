library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(rubix)
library(glitter)

outputPath <- "~/GitHub/chemidplusData/data-raw/"

conn <- chariot::connectAthena()
chemiTables <-
        pg13::lsTables(conn = conn,
                 schema = "chemidplus")
chemiData <-
chemiTables %>%
        rubix::map_names_set(~pg13::readTable(conn = conn,
                                              schema = "chemidplus",
                                              tableName = .))

chariot::dcAthena(conn = conn)


outputPaths <- paste0(outputPath, names(chemiData), ".csv")

chemiData %>%
        purrr::map2(outputPaths,
                    function(x,y) broca::simply_write_csv(x = x,
                                                          file = y))


loadLines <-
chemiTables %>%
        purrr::map2(basename(outputPaths),
                    function(x, y) paste0(x, " <- broca::simply_read_csv('data-raw/", y, "')")) %>%
        unlist()

usethisLines <-
        paste0("usethis::use_data(", paste(chemiTables, collapse = ","), ", overwrite = TRUE)")

readr::write_lines(c("library(broca)",
                     loadLines,
                     usethisLines),
                   path = paste0(outputPath, "DATASET.R"))

setwd("~/GitHub/chemidplusData/")
source("data-raw/DATASET.R",
       local = TRUE)
glitter::docPushInstall(commit_message = "automated data refresh", has_vignettes = FALSE)

chariot::dcAthena(conn = conn)
