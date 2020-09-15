#' @title
#' Does the URL return no records found?
#' @param response xml_document xml_node class object
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#' @rdname isNoRecord
#' @export
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
