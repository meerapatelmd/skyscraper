#' @title
#' Is the RN URL returning a HTTP error 404?
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @param rn_url PARAM_DESCRIPTION
#' @param sleep_time PARAM_DESCRIPTION, Default: 3
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate_all}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[centipede]{strsplit}}
#'  \code{\link[purrr]{map2}},\code{\link[purrr]{set_names}},\code{\link[purrr]{map}}
#' @export
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom rvest html_nodes html_text
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr transmute distinct mutate bind_rows mutate_at
#' @importFrom stringr str_remove_all
#' @importFrom centipede strsplit
#' @importFrom purrr map2 set_names map
#' @importFrom magrittr %>%

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
