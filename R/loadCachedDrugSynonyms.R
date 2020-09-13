#' Scrape Drug Page
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @import secretary
#' @import xml2
#' @export

loadCachedDrugSynonyms <-
    function(df, output.var = "loadCachedDrugSynonyms_results") {

            if (exists(output.var, envir = globalenv())) {
                    input <- get(output.var, envir = globalenv())
                    starting_index <- length(input)
            } else {
                    starting_index <- 1
            }

            df <-
                    df %>%
                    dplyr::rename_all(tolower)

            #scrapeDrugSynonyms_output <- tibble::tibble()

            # pb <- progress::progress_bar$new(total = nrow(df),
            #                                  format = ":what [:bar] :current/:total (:percent)")
            #
            # pb$tick(0)
            # Sys.sleep(0.2)

            output <- list()

            for (i in 1:nrow(df)) {

                    secretary::typewrite(paste0("[", Sys.time(), "]"), "\t", i, " of ", nrow(df))



                    drug_link <- df$drug_def_link[i]
                    drug_name <- df$drug[i]

                    # pb$tick(tokens = list(what = drug_name))
                    # Sys.sleep(0.2)


                    output[[i]] <- loadCachedScrape(url = drug_link) %>%
                                        dplyr::bind_rows()

                    names(output)[i] <- drug_name
                    # if (is.null(results)) {
                    #         results <-
                    #                        tryCatch(
                    #                                         xml2::read_html(drug_link) %>%
                    #                                                 rvest::html_nodes("table") %>%
                    #                                                 rvest::html_table(),
                    #                                         error = function(e) tibble::tibble(X1 = NA)
                    #                         )
                    #
                    #         Sys.sleep(0.2)
                    #
                    #         cacheScrape(object = results,
                    #                     url = drug_link)
                    # }
                    #
                    assign(x = output.var,
                           value = output,
                           envir = globalenv())
            }

            secretary::typewrite_bold("Completed.")
    }

