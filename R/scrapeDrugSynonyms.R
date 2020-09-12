#' Scrape Drug Page
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @import secretary
#' @import xml2
#' @export

scrapeDrugSynonyms <-
    function(df) {

            #scrapeDrugSynonyms_output <- tibble::tibble()

            pb <- progress::progress_bar$new(total = nrow(df),
                                             format = ":what [:bar] :current/:total (:percent)")

            pb$tick(0)
            Sys.sleep(0.2)

            for (i in 1:nrow(df)) {

                    drug_link <- df$drug_def_link[i]
                    drug_name <- df$drug[i]

                    pb$tick(tokens = list(what = drug_name))
                    Sys.sleep(0.2)


                    results <- loadCachedScrape(url = drug_link)

                    if (is.null(results)) {
                            results <-
                                           tryCatch(
                                                            xml2::read_html(drug_link) %>%
                                                                    rvest::html_nodes("table") %>%
                                                                    rvest::html_table(),
                                                            error = function(e) tibble::tibble(X1 = NA)
                                            )

                            Sys.sleep(0.2)

                            cacheScrape(object = results,
                                        url = drug_link)
                    }

                    scrapeDrugSynonyms_output <-
                            dplyr::bind_rows(scrapeDrugSynonyms_output,
                                            dplyr::bind_rows(results) %>%
                                                                dplyr::mutate(drug = drug_name))
            }

            scrapeDrugSynonyms_output
    }

