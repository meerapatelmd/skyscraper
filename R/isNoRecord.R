
#' @param response "xml_document" "xml_node"

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
