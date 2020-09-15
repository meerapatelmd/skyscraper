




isMultipleHits <-
        function(response) {

                response %>%
                        rvest::html_nodes(".bodytext") %>%
                        rvest::html_text() %>%
                        tibble::as_tibble_col(column_name = "multiple_match") %>%
                        rubix::filter_at_grepl(multiple_match,
                                               grepl_phrase = "MW[:]{1} ",
                                               evaluates_to = FALSE) %>%
                        tidyr::extract(col = multiple_match,
                                       into = c("compound_match", "rn"),
                                       regex = "(^.*?) \\[.*?\\](.*$)") %>%
                        dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn)) %>%
                        dplyr::mutate_all(stringr::str_remove_all, "No Structure")

        }
