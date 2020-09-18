#' Get the Links to Drug Pages
#' @import xml2
#' @import rvest
#' @import tibble
#' @import dplyr
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @export

nciDrugDetailLinks <-
    function(max_page = 50) {

            .Deprecated("get_drug_detail_links")

            output <- list()

            for (i in 1:max_page) {

                            drug_def_scrape <- xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

                            # Stop if there is no more information
                            no_data_message <-
                            drug_def_scrape %>%
                                    rvest::html_nodes("#ctl36_ctl00_resultListView_ctrl0_lblNoDataMessage") %>%
                                    rvest::html_text()


                            if (length(no_data_message) == 0) {


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
                                    names(output)[i] <- as.character(i)

                                }

            }

            .output <- dplyr::bind_rows(output, .id = "PAGE") %>%
                    dplyr::mutate(DRUG_DEF_LINK = paste0("https://www.cancer.gov", DRUG_DEF_LINK))


            return(.output)

    }

