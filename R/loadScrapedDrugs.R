#' Scrape Drug Page
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @import secretary
#' @import xml2
#' @export

loadScrapedDrugs <-
    function(.input,
             starting_row,
             ending_row) {

            .output <- list()
            for (i in starting_row:ending_row) {

                secretary::typewrite(paste0("[", Sys.time(), "]"), "\tLoading ", i, " of ", nrow(.input))

                .output[[i]] <-
                loadCachedScrape(page=i,
                                 url= .input$DRUG_DEF_LINK[i],
                                 source="cancergov")

                if (is.null(.output[[i]])) {

                            .output[[i]] <-
                                    xml2::read_html(.input$DRUG_DEF_LINK[i]) %>%
                                                rvest::html_nodes("dl") %>%
                                                rvest::html_text()

                            if (!is.logical(.output[[i]])) {

                                        cacheScrape(object=.output[[i]],
                                                         page=i,
                                                         url=.input$DRUG_DEF_LINK[i],
                                                         source="cancergov")

                            }
                            Sys.sleep(.1)

                }



            }
            return(.output)

    }
