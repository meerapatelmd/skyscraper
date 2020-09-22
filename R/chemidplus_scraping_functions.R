#' @param conn          Postgres connection object
#' @param rn_url        Registry number URL to read
#' @param response      (optional) "xml_document" "xml_node" class object returned by xml2::read_html for the `rn_url` argument. Providing a response from a single HTML read reduces the chance of encountering a HTTP 503 error when parsing multiple sections from a single URL. If a response argument is missing, a response is read. Followed by the `sleep_time` in seconds.
#' @param schema        Schema that the returned data is written to, Default: 'chemidplus'
#' @param sleep_time    If the response argument is missing, the number seconds to pause after reading the URL, Default: 3
#' @name chemidplus_scraping_functions
NULL
