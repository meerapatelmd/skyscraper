#' Load Scraped Cancer.gov Drug Dictionary Data
#' @import rubix
#' @import dplyr
#' @import tibble
#' @import centipede
#' @import stringr
#' @export

loadScrapedDrugDict <-
        function(max_page = 39) {

            urls <- paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", 1:max_page)

            .output <-
            1:max_page %>%
                purrr::map2(urls, function(x,y) loadCachedScrape(page=x,
                                                                  url=y,
                                                        source="cancergov")) %>%
                rubix::map_names_set(function(x) tibble(DRUG = unlist(x$DRUG),
                                                        DEFINITION = unlist(x$DEFINITION))) %>%
                dplyr::bind_rows(.id = "Page") %>%
                dplyr::mutate_all(stringr::str_remove_all, "[\r\n\t]") %>%
                dplyr::mutate_all(centipede::trimws) %>%
                dplyr::mutate_all(stringr::str_replace_all, "(^.*results found for: ALL )(.*$)", "\\2")

            return(.output)

        }
