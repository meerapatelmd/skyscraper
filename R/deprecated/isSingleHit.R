#' @title
#' Parse the RN from a single Substance Page
#'
#'
#' @inherit chemidplus_parsing_functions description
#'
#' @inheritSection chemidplus_parsing_functions Single Hit
#'
#' @inheritParams chemidplus_parsing_functions
#'
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[centipede]{strsplit}}
#'  \code{\link[tibble]{as_tibble}},\code{\link[tibble]{tribble}}
#'  \code{\link[tidyr]{extract}},\code{\link[tidyr]{pivot_wider}}
#'  \code{\link[dplyr]{mutate}}
#'  \code{\link[stringr]{str_remove}}
#'
#' @rdname isSingleHit
#'
#' @family chemidplus parsing
#'
#' @export
#'
#' @importFrom rvest html_node html_text
#' @importFrom centipede strsplit
#' @importFrom tibble as_tibble_col tribble
#' @importFrom tidyr extract pivot_wider
#' @importFrom dplyr transmute mutate
#' @importFrom stringr str_remove_all
#' @importFrom magrittr %>%


isSingleHit <-
        function(response) {

                output <-
                        response %>%
                        rvest::html_node("h1") %>%
                        rvest::html_text()


                if (!is.na(output)) {
                        output %>%
                                centipede::strsplit(split = "Substance Name[:]{1}|RN[:]{1}|UNII[:]{1}|InChIKey[:]{1}", type = "before") %>%
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
