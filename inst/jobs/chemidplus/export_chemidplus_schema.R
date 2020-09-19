library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(rubix)
library(glitter)
library(sinew)


current_wd <- getwd()
target_dir <- path.expand("~/GitHub/chemidplusData/")
setwd(target_dir)
outputPath <- paste0(target_dir, "data-raw/")

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
                    function(x, y) paste0(x, " <- broca::simply_read_csv('", outputPath, y, "')")) %>%
        unlist()

# setWdLines <-
#         paste0("\nsetwd('",dirname(outputPath), "')")

usethisLines <-
        paste0("\nusethis::use_data(", paste(chemiTables, collapse = ","), ", overwrite = TRUE)")

readr::write_lines(c("library(broca)\n",
                     loadLines,
                     #setWdLines,
                     usethisLines),
                   path = paste0(outputPath, "DATASET.R"))

setwd(target_dir)
source(paste0(outputPath, "DATASET.R"),
       local = FALSE)

secretary::press_enter()

if (interactive()) {
        data_dir <- paste0(target_dir, "data")
        cave::create_dir_if_not_exist(data_dir)

        current_files <- list.files("data", full.names = T)
        new_location <-
                stringr::str_replace_all(current_files,
                                         pattern = "(^.*data)([/]{1}.*)",
                                         replacement = paste0(data_dir, "\\2"))


        mapply(file.copy, from = current_files, to = new_location, overwrite = TRUE)


}


paste0("chemiData$", names(chemiData)) %>%
        purrr::map(makeOxygen, print = FALSE) %>%
        purrr::map2(names(chemiData), function(x,y)
                stringr::str_replace(string = x,
                                     pattern = "(.*\")(.*)(\")",
                                     replacement = paste0("\\1", y, "\\3"))) %>%
        purrr::map2(names(chemiData), function(x,y)
                stringr::str_replace(string = x,
                                     pattern = "DATASET_TITLE",
                                     replacement = y)) %>%
        unlist() %>%
        paste(collapse = "\n\n") %>%
        cat(file = "~/GitHub/chemidplusData/R/data.R")


glitter::docPushInstall(commit_message = "automated data refresh", has_vignettes = FALSE)

setwd(current_wd)

