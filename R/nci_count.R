#' @title
#' Get the NCI Drug Dictionary Count
#' @description
#' Get the total number of drugs in the NCI Drug Dictionary
#' @return
#' An integer in the "X results found for: ALL" at "https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=1"
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#' @rdname nci_count
#' @export
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_text
#' @importFrom magrittr %>%

nci_count <-
    function() {
            page_scrape <-
                        xml2::read_html("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=1")

                page_scrape %>%
                rvest::html_nodes("#ctl36_ctl00_lblNumResults") %>%
                rvest::html_text() %>%
                        as.integer()

    }
