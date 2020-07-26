#' Get the Links to Drug Pages
#' @import xml2
#' @import rvest
#' @import tibble
#' @import dplyr
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @export

getDrugPageLinks <-
    function(max_page = 39) {

            output <- list()

            for (i in 1:max_page) {

                    drug_def_scrape <- xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

                    drug_def_link <-
                            drug_def_scrape %>%
                            rvest::html_nodes("dfn") %>%
                            rvest::html_children() %>%
                            rvest::html_attr(name = "href")

                    drug_names <-
                            drug_def_scrape %>%
                            rvest::html_nodes("dfn") %>%
                            rvest::html_text() %>%
                            trimws()

                    output[[i]] <- tibble::tibble(DRUG = drug_names,
                                          DRUG_DEF_LINK = drug_def_link)

            }

            .output <- dplyr::bind_rows(output) %>%
                        dplyr::mutate(DRUG_DEF_LINK = paste0("https://www.cancer.gov", DRUG_DEF_LINK))


            return(.output)

    }

