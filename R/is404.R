#' @title
#' Is the RN URL returning an HTTP error 404?
#'
#' @description
#' This function gets a response from an RN URL and checks for a 404 message in the error output.
#'
#' @param       rn_url  Registry Number URL
#'
#' @return
#' logical vector of length 1
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'
#' @rdname is404
#'
#' @family chemidplus parsing
#'
#' @export
#'
#' @importFrom xml2 read_html

is404 <-
        function(rn_url) {

                # rn_url <- "https://chem.nlm.nih.gov/chemidplus/rn/startswith/499313-74-3"

                response <-
                        tryCatch(
                                xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE")),
                                error = function(e) {
                                        return(toString(e))
                                }
                        )


                if (is.character(response)) {

                        grepl("HTTP error 404",
                              response)

                } else {

                        FALSE

                }

        }
