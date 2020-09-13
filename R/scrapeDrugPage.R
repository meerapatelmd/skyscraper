#' @title
#' Scrape a Drug HTML Page
#'
#' @description
#' Scrape the Tables in the Drug Page of the NCI Drug Dictionary if one exists
#'
#' @param df                    Data Frame of any dimensions with a 'Drug' and 'Drug_Def_Link' fields that correspond to the drug and its hyperlink
#' @param random                If TRUE, will sort the df parameter randomly before executing. This is helpful when running the scrape simulataneously to save time, with one job scraping random and the other scraping in the order that it was provided in df. Default: TRUE
#' @param progress_bar          If TRUE, a progress bar is returned in the console if the function is being run interactively. Default: TRUE
#'
#' @return
#' The scrape results are cached using the cacheScrape function by Drug HTML. Other than the progress messages in the console, nothing is returned. Regardless of whether or not the entire length of the drug links have been cached, any cached results can be loaded usng the loadCachedDrugPage function.
#'
#' @details
#' The link to the drug page is scraped as a dataframe that by default are named "X1" and "X2". "X1" is the type of synonym on the drug page while "X2" is the value.
#' If an error is encountered at time of scraping, the values of both X1 and X2 default to NA. If no values were returned on scrape, the scrape returns a NULL. All scrape results are cached using the R.cache package with the directory set to "skyscraper"
#'
#' @seealso
#'  \code{\link[dplyr]{select_all}}
#'  \code{\link[tibble]{rownames}},\code{\link[tibble]{c("tibble", "tibble")}}
#'  \code{\link[progress]{progress_bar}}
#'  \code{\link[secretary]{typewrite}},\code{\link[secretary]{character(0)}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#'
#' @rdname scrapeDrugPage
#'
#' @export
#' @importFrom dplyr rename_all
#' @importFrom tibble rowid_to_column tibble
#' @importFrom progress progress_bar
#' @importFrom secretary typewrite redTxt
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_table
#' @importFrom magrittr %>%

scrapeDrugPage <-
    function(df,
             random = TRUE,
             progress_bar = TRUE) {

            df <-
                    df %>%
                    dplyr::rename_all(tolower)

            if (random) {

                df <-
                        df %>%
                            tibble::rowid_to_column("sample_id")

                df <- df[sample(1:nrow(df)), ]
            }


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

    }

