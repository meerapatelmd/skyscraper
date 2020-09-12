#' Scrape Cancer.gov Drug Dictionary
#' @import httr
#' @import jsonlite
#' @import rvest
#' @import secretary
#' @param max_page Maximum page to scrape until at https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @export

scrapeNCIDrugDict <-
    function(max_page = 39) {

        for (i in 1:max_page) {

                if (i == 1) {
                        output <- tibble::tibble()
                }

                secretary::typewrite(paste0("[", Sys.time(), "]"), "\t", i, " of ", max_page)

                page_scrape <- xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

                # page_text <-
                #         page_scrape %>%
                #         rvest::html_nodes("#main") %>%
                #         rvest::html_text()

                page_text <-
                        page_scrape %>%
                        rvest::html_nodes(".dictionary-list") %>%
                        rvest::html_text()

                page_text2 <-
                    strsplit(page_text, split = "\r\n            \r\n        \r\n            \r\n               \r\n                    \r\n                        ") %>%
                    unlist()

                page_text3 <-
                    page_text2 %>%
                    purrr::map(strsplit, split = "\r\n                \r\n            \r\n            \r\n            \r\n                 ") %>%
                    purrr::map(unlist) %>%
                    purrr::map(unlist)

                qa1 <-
                    page_text3 %>%
                    purrr::keep(function(x) length(x) != 2)

                if (length(qa1) > 0) {

                        qaScrapeCancerGovDict <<- qa1

                        stop("All drug-definition combinations did not parse. See qaScrapeCancerGovDict object.")

                }

                page_text4 <-
                    page_text3 %>%
                    purrr::transpose()

                names(page_text4) <- c("DRUG", "DEFINITION")


                output <-
                        dplyr::bind_rows(output,
                                         tibble::tibble(DRUG = unlist(page_text4$DRUG),
                                                        DEFINITION = unlist(page_text4$DEFINITION)))

                # cacheScrape(page_text4,
                #             page=i,
                #             url=paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i),
                #             source="cancergov")

        }
            output %>%
                    dplyr::mutate_all(trimws) %>%
                    dplyr::mutate_all(stringr::str_replace_all, pattern = "[\r\n\t]{1,}|[ ]{2,}", replacement = " ")

    }
