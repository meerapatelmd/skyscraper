#' @title
#' Scrape the NCI Drug Dictionary
#'
#' @description
#' Scrape the drug names and their definitions from all the pages of the NCI Drug Dictionary (https://www.cancer.gov/publications/dictionaries/cancer-drug)
#'
#' @param max_page Maximum page to scrape, Default: 39
#'
#' @return
#' A data frame
#'
#' @seealso
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[secretary]{typewrite}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{keep}},\code{\link[purrr]{transpose}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate_all}}
#'  \code{\link[stringr]{str_replace}}
#'
#' @export
#' @importFrom tibble tibble
#' @importFrom secretary typewrite
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_text
#' @importFrom purrr map keep transpose
#' @importFrom dplyr bind_rows mutate_all
#' @importFrom stringr str_replace_all
#' @importFrom magrittr %>%



get_drug <-
    function(max_page = 50) {

        for (i in 1:max_page) {

                if (i == 1) {
                        output <- tibble::tibble()
                }

                page_scrape <- xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

                no_data_message <-
                        page_scrape %>%
                        rvest::html_nodes("#ctl36_ctl00_resultListView_ctrl0_lblNoDataMessage") %>%
                        rvest::html_text()


                if (length(no_data_message) == 0) {

                                page_text <-
                                        page_scrape %>%
                                        rvest::html_nodes(".dictionary-list dd") %>%
                                        rvest::html_text()

                                page_scrape %>%
                                        rvest::html_nodes(".dictionary-list, a") %>%
                                        rvest::html_text()



                                drugs <-
                                        page_scrape %>%
                                        rvest::html_nodes("a") %>%
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

    }
