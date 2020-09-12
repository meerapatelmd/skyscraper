#' Scrape Drug Page
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @import secretary
#' @import xml2
#' @export

scrapeDrugSynonymsRandom <-
    function(df) {


            if (interactive()) {

                            #scrapeDrugSynonyms_output <- tibble::tibble()

                            pb <- progress::progress_bar$new(total = nrow(df),
                                                             format = ":what [:bar] :current/:total (:percent)")

                            pb$tick(0)
                            Sys.sleep(0.2)

                            df <-
                                    df %>%
                                    tibble::rowid_to_column("sample_id")

                            samples <- df$sample_id

                            while (length(samples)) {
                                    target_sample_id <- sample(samples, size = 1, replace = F)

                                    drug_link <- df$drug_def_link[target_sample_id]
                                    drug_name <- df$drug[target_sample_id]

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

                                    samples <- samples[!(samples %in% target_sample_id)]

                                    pb$tick()
                                    Sys.sleep(0.2)
                            }
            } else {

                    df <-
                            df %>%
                            tibble::rowid_to_column("sample_id")

                    samples <- df$sample_id

                    while (length(samples)) {

                            target_sample_id <- sample(samples, size = 1, replace = F)
                            secretary::typewrite(paste0("[", Sys.time(), "]"), "\tStarting ", target_sample_id, " of ", nrow(df))

                            drug_link <- df$drug_def_link[target_sample_id]
                            drug_name <- df$drug[target_sample_id]



                            results <- loadCachedScrape(url = drug_link)

                            if (is.null(results)) {
                                    secretary::typewrite(paste0("[", Sys.time(), "]"), "\tScraping ", target_sample_id, " of ", nrow(df))
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
                            } else {
                                    secretary::typewrite(paste0("[", Sys.time(), "]"), target_sample_id, " of ", nrow(df), " is cached.")
                            }

                            samples <- samples[!(samples %in% target_sample_id)]
                            secretary::typewrite(paste0("[", Sys.time(), "]"), length(samples), " of a total of ", nrow(df), " remaining.")
                    }


            }
    }

