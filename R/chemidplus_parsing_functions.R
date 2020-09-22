#' @title
#' ChemiDPlus Parsing Functions
#'
#' @description
#' There are 2 HTML responses involved in ChemiDPlus scraping. The first HTML response is associated with the concept search while the second is for the 1 to 5 Registry Numbers and their URLs if records were found. The ChemiDPlus parsing functions delineate the response into 3 groups: searches the resulted in no records, a single record, or multiple records (currently limited to parsing a maximum of 5 records).
#'
#' @param response "xml_document" "xml_node" class object returned by xml2::read_html
#'
#' @name chemidplus_parsing_functions
NULL

