#' Scrape Drug Page
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @import secretary
#' @import xml2
#' @export

scrapeDrugSynonymsRandom <-
    function(df, progress_bar = TRUE) {

                df <-
                        df %>%
                            dplyr::rename_all(tolower) %>%
                            tibble::rowid_to_column("sample_id")

                df <- df[sample(1:nrow(df)), ]


                if (progress_bar) {
                    pb <- progress::progress_bar$new(total = nrow(df),
                                                     format = ":what [:bar] :current/:total (:percent)")

                    pb$tick(0)
                    Sys.sleep(0.2)
                }


                for (i in 1:nrow(df)) {
                        secretary::typewrite(paste0("[", Sys.time(), "]"), "Starting", i, "of", nrow(df))

                        drug_link <- df$drug_def_link[i]
                        drug_name <- df$drug[i]


                        if (progress_bar) {

                                pb$tick(tokens = list(what = drug_name))
                                Sys.sleep(0.2)

                        }

                        results <- loadCachedScrape(url = drug_link)

                        if (is.null(results)) {

                                secretary::typewrite(paste0("[", Sys.time(), "]"), secretary::redTxt("Scraping", i, "of", nrow(df)))

                                results <-
                                        tryCatch(
                                                xml2::read_html(drug_link) %>%
                                                        rvest::html_nodes("table") %>%
                                                        rvest::html_table(),
                                                error = function(e) tibble::tibble(X1 = NA)
                                        )


                                cacheScrape(object = results,
                                            url = drug_link)

                        } else {
                                secretary::typewrite(paste0("[", Sys.time(), "]"), i, "of", nrow(df), "is cached.")
                        }

                        secretary::typewrite(paste0("[", Sys.time(), "]"), (nrow(df)-i), "of a total of", nrow(df), "remaining.")
                }

#
#
#                 while (length(samples) > 0) {
#
#                             target_sample_id <- sample(samples, size = 1, replace = F)
#                             secretary::typewrite(paste0("[", Sys.time(), "]"), "Starting", target_sample_id, "of", nrow(df))
#
#                             drug_link <- df$drug_def_link[target_sample_id]
#                             drug_name <- df$drug[target_sample_id]
#
#
#                             if (progress_bar) {
#
#                                     pb$tick(tokens = list(what = drug_name))
#                                     Sys.sleep(0.2)
#
#                             }
#
#
#                             results <- loadCachedScrape(url = drug_link)
#
#                             if (is.null(results)) {
#
#                                     secretary::typewrite(paste0("[", Sys.time(), "]"), secretary::redTxt("Scraping", target_sample_id, "of", nrow(df)))
#
#                                     results <-
#                                             tryCatch(
#                                                     xml2::read_html(drug_link) %>%
#                                                     rvest::html_nodes("table") %>%
#                                                     rvest::html_table(),
#                                                     error = function(e) tibble::tibble(X1 = NA)
#                                             )
#
#
#                                     cacheScrape(object = results,
#                                                 url = drug_link)
#
#                             } else {
#                                     secretary::typewrite(paste0("[", Sys.time(), "]"), target_sample_id, "of", nrow(df), "is cached.")
#                             }
#
#                             samples <- samples[!(samples %in% target_sample_id)]
#                             secretary::typewrite(paste0("[", Sys.time(), "]"), length(samples), "of a total of", nrow(df), "remaining.")
#                             if (progress_bar) {
#
#                                     pb$tick()
#                                     Sys.sleep(0.2)
#
#                             }
#                     }
    }

