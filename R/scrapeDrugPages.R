#' Scrape Drug Page
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @import secretary
#' @import xml2
#' @export

scrapeDrugPages <-
    function(max_page) {

            .input <- getDrugPageLinks(max_page = max_page)

            for (i in 1:nrow(.input)) {

                secretary::typewrite(paste0("[", Sys.time(), "]"), "\t", i, " of ", nrow(.input))

                cached <-
                loadCachedScrape(page=i,
                                 url= .input$DRUG_DEF_LINK[i],
                                 source="cancergov")

                if (is.null(cached)) {

                            .output <- xml2::read_html(.input$DRUG_DEF_LINK[i]) %>%
                                                            rvest::html_nodes("dl") %>%
                                                            rvest::html_text()

                            if (!is.null(.output)) {
                                cacheScrape(object=.output,
                                                 page=i,
                                                 url=.input$DRUG_DEF_LINK[i],
                                                 source="cancergov")
                            }
                            Sys.sleep(.1)

                }
            }
    }
