#' @title
#' CancerGov Functions
#'
#' @description
#' These functions scrape and parse the NCI Drug Dictionary found at CancerGov.org.
#'
#' @section
#' Web Source Types:
#' The NCI Drug Dictionary has 2 data sources that run in parallel. The first source is the Drug Dictionary itself at \href{https://www.cancer.gov/publications/dictionaries/cancer-drug}{https://www.cancer.gov/publications/dictionaries/cancer-drug}. The other source are the individual drug pages, called Drug Detail Links in skyscraper, that contain tables of synonyms, including investigational names.
#'
#' @section
#' Drug Dictionary:
#' The listed drug names and their definitions are scraped from the Drug Dictionary HTML and updated to a Drug Dictionary Table in a `cancergov` schema.
#'
#' @section
#' Drug Detail Links:
#' The links to Drug Pages are scraped from the Data Dictionary URL over the maximum page number and are saved to a Drug Link Table in the `cancergov` schema. The URLs in the Drug Link Table are then scraped for any HTML Tables of synonyms and the results are written to a Drug Link Synonym Table.
#'
#'
#' @param conn Postgres connection object
#' @param max_page maximum page number to iterate the scrape over in the "https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=" path, Default: 50
#'
#'
#' .
#'
#' @name cancergov_functions
NULL
