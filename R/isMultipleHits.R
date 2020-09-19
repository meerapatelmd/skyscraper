#' @title
#' Get the RN Number an URL for a Phrase
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[rubix]{filter_at_grepl}}
#'  \code{\link[tidyr]{extract}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{mutate_all}}
#'  \code{\link[stringr]{str_remove}}
#' @rdname isMultipleHits
#' @export
#' @importFrom rvest html_nodes html_text
#' @importFrom tibble as_tibble_col
#' @importFrom rubix filter_at_grepl
#' @importFrom tidyr extract
#' @importFrom dplyr mutate mutate_all
#' @importFrom stringr str_remove_all
#' @importFrom magrittr %>%


isMultipleHits <-
        function(response) {

                output <-
                response %>%
                        rvest::html_nodes(".bodytext") %>%
                        rvest::html_text()


                chem_names <-
                response %>%
                        rvest::html_nodes(".chem-name") %>%
                        rvest::html_text() %>%
                        paste(collapse = "|")

                output_a <-
                        police::try_catch_error_as_null(
                        output  %>%
                        tibble::as_tibble_col(column_name = "multiple_match") %>%
                        rubix::filter_at_grepl(multiple_match,
                                               grepl_phrase = "MW[:]{1} ",
                                               evaluates_to = FALSE) %>%
                        tidyr::extract(col = multiple_match,
                                       into = c("compound_match", "rn"),
                                       regex = paste0("(^", chem_names, ") \\[.*?\\](.*$)")) %>%
                        dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn)) %>%
                        dplyr::mutate_all(stringr::str_remove_all, "No Structure") %>%
                        dplyr::filter_at(vars(compound_match,
                                              rn),
                                         all_vars(!is.na(.))))


                if (is.null(output_a)) {

                        chem_name_vector <-
                                response %>%
                                rvest::html_nodes(".chem-name") %>%
                                rvest::html_text()


                                output  %>%
                                                tibble::as_tibble_col(column_name = "multiple_match") %>%
                                                rubix::filter_at_grepl(multiple_match,
                                                                       grepl_phrase = "MW[:]{1} ",
                                                                       evaluates_to = FALSE) %>%
                                dplyr::mutate(compound_match = chem_name_vector) %>%
                                dplyr::mutate(nchar_compound_name = nchar(compound_match)) %>%
                                dplyr::mutate(string_start_rn = nchar_compound_name+1) %>%
                                dplyr::mutate(total_nchar = nchar(multiple_match)) %>%
                                dplyr::mutate(rn = substr(multiple_match, string_start_rn, total_nchar)) %>%
                                dplyr::mutate_all(stringr::str_remove_all, "No Structure") %>%
                                rubix::rm_multibyte_chars() %>%
                                dplyr::filter_at(vars(compound_match,
                                                      rn),
                                                 all_vars(!is.na(.))) %>%
                                dplyr::distinct() %>%
                                tibble::as_tibble() %>%
                                rubix::normalize_all_to_na() %>%
                                dplyr::transmute(compound_match,
                                              rn,
                                              rn_url = ifelse(!is.na(rn),
                                                              paste0("https://chem.nlm.nih.gov/chemidplus/rn/", rn),
                                                              NA))


                } else {

                output_b <-
                        output  %>%
                                tibble::as_tibble_col(column_name = "multiple_match") %>%
                                rubix::filter_at_grepl(multiple_match,
                                                       grepl_phrase = "MW[:]{1} ",
                                                       evaluates_to = FALSE) %>%
                                tidyr::extract(col = multiple_match,
                                               into = c("compound_match", "rn"),
                                               regex = paste0("(^", chem_names, ")([0-9]{1,}[-]{1}[0-9]{1,}[-]{1}[0-9]{1,}.*$)")) %>%
                                dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn)) %>%
                                dplyr::mutate_all(stringr::str_remove_all, "No Structure") %>%
                                dplyr::filter_at(vars(compound_match,
                                                      rn),
                                                 all_vars(!is.na(.)))

                output <-
                        dplyr::bind_rows(output_a,
                                 output_b)  %>%
                        dplyr::distinct()


                if (nrow(output)) {
                        output %>%
                        tibble::as_tibble() %>%
                        rubix::normalize_all_to_na() %>%
                        dplyr::transmute(compound_match,
                                         rn,
                                         rn_url = ifelse(!is.na(rn),
                                                         paste0("https://chem.nlm.nih.gov/chemidplus/rn/", rn),
                                                         NA))
                } else {
                        tibble::tribble(~compound_match, ~rn, ~rn_url)
                }
                }

        }
