#' Scrape Cancer.gov Drug Dictionary
#' @import httr
#' @import jsonlite
#' @import rvest
#' @import secretary
#' @param max_page Maximum page to scrape until at https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @export

nciDrugCount <-
    function() {
            .Deprecated("nci_count")

                i <- 1

                page_scrape <-
                        xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

                page_scrape %>%
                rvest::html_nodes("#ctl36_ctl00_lblNumResults") %>%
                rvest::html_text() %>%
                        as.integer()

    }
