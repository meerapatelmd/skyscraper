#' @title
#' ChemiDPlus Scraping Functions
#'
#' @description
#' All ChemiDPlus Scraping Functions operate on a Registry Number URL (`rn_url`). The major sections found at the ChemiDPlus site are: "Names and Synonyms", "Classification", "Registry Numbers", "Links to Resources".
#'
#' @return
#' Each section is parsed by a respective skyscraper function that stores the scraped results in a table of the same name in a schema. If a connection argument is not provided, the results are returned as a dataframe in the R console.
#'
#' @section
#' Names and Synonyms:
#' The "Names and Synonyms" Section scraped results contain a Timestamp, RN URL (Identifier). If the section has subheadings, the subheading is scraped as the Synonym Type along with the Synonym itself.
#'
#' @param conn          Postgres connection object
#' @param rn_url        Registry number URL to read
#' @param response      (optional) "xml_document" "xml_node" class object returned by xml2::read_html for the `rn_url` argument. Providing a response from a single HTML read reduces the chance of encountering a HTTP 503 error when parsing multiple sections from a single URL. If a response argument is missing, a response is read. Followed by the `sleep_time` in seconds.
#' @param schema        Schema that the returned data is written to, Default: 'chemidplus'
#' @param sleep_time    If the response argument is missing, the number seconds to pause after reading the URL, Default: 3
#'
#' @name chemidplus_scraping_functions
NULL
