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

                        output  %>%
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



isSingleHit <-
        function(response) {

                output <-
                response %>%
                        rvest::html_node("h1") %>%
                        rvest::html_text()


                if (!is.na(output)) {
                        output %>%
                        centipede::strsplit(split = "Substance Name[:]{1}|RN[:]{1}|UNII[:]{1}", type = "before") %>%
                        unlist() %>%
                        tibble::as_tibble_col(column_name = "h1") %>%
                        tidyr::extract(col = h1,
                                        into = c("identifier_type", "identifier"),
                                        regex = "(^.*?)[:]{1}(.*$)") %>%
                        tidyr::pivot_wider(names_from = identifier_type,
                                           values_from = identifier) %>%
                        dplyr::transmute(compound_match = `Substance Name`,
                                         rn = stringr::str_remove_all(RN, "\\s{1,}")) %>%
                        dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn))
                } else {
                        tibble::tribble(~compound_match,
                                        ~rn,
                                        ~rn_url)
                }
        }
