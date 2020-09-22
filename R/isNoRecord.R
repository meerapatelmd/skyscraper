#' @title
#' Does the RN URL indicate that no records were found?
#'
#' @inherit chemidplus_parsing_functions description
#'
#' @inheritSection chemidplus_parsing_functions No Record
#'
#' @inheritParams chemidplus_parsing_functions
#'
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'
#' @rdname isNoRecord
#'
#' @family chemidplus parsing
#'
#' @export
#'
#' @importFrom rvest html_nodes html_text
#' @importFrom magrittr %>%

isNoRecord <-
        function(response) {

                result <-
                response %>%
                        rvest::html_nodes("h3") %>%
                        rvest::html_text()

                if (length(result) == 1) {

                        if (result == "The following query produced no records:") {

                                TRUE

                        } else {
                                FALSE
                        }
                } else {
                        FALSE
                }
        }
