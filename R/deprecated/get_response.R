#' @title
#' Get a Response from a RN URL
#' @description (Deprecated) FUNCTION_DESCRIPTION
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
#' @rdname get_response
#' @export
#' @importFrom xml2 read_html

get_response <-
        function(rn_url,
                 sleep_time = 3) {

                .Deprecated("scrape_cdp")

                Sys.sleep(sleep_time)
                xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))
        }
