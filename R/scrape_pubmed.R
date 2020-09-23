






scrape_pubmed <-
        function() {
                pubmed_scrape <- xml2::read_html("https://pubmed.ncbi.nlm.nih.gov/?cmd=search&cmd_current=Limits&term=AZD5153")

                results_count <-
                        pubmed_scrape %>%
                        rvest::html_node(".results-amount") %>%
                        rvest::html_text() %>%
                        stringr::str_remove_all(pattern = "[^0-9]") %>%
                        as.integer()


                pub_snippet <-
                        pubmed_scrape %>%
                        rvest::html_nodes(".full-view-snippet") %>%
                        rvest::html_text(trim = TRUE)

                pub_citation <-
                        pubmed_scrape %>%
                        rvest::html_nodes(".full-journal-citation") %>%
                        rvest::html_text(trim = TRUE)

                pub_title <-
                        pubmed_scrape %>%
                        rvest::html_nodes(".docsum-title") %>%
                        rvest::html_text(trim = TRUE)
        }
