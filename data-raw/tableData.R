## code to prepare `tableData` dataset goes here
library(devtools)
devtools::install_github("meerapatelmd/broca")
tableData <- broca::read_full_excel("data-raw/tableData.xlsx")
usethis::use_data(tableData, overwrite = TRUE)
