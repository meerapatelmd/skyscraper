#' @title
#' Scrape
#'
#' @description
#' Read html with a crawl delay set with the `sleep_time` argument. In addition, if the initial response returns NULL, the url is read again. A NULL will be returned if the second try also results in an error. All connections are closed before the response is returned.
#'
#' @inheritParams xml2::read_html
#' @param sleep_time    Time in seconds for the system to sleep before each scrape with \code{\link[xml2]{read_html}}.
#' @rdname scrape
#' @family scrape functions
#' @export


scrape <-
        function(x,
                 encoding = "",
                 ...,
                 options = c("RECOVER", "NOERROR", "NOBLANKS"),
                 sleep_time = 3,
                 verbose = TRUE) {


                if (verbose) {
                        secretary::typewrite("Sleeping...")
                }

                Sys.sleep(sleep_time)

                if (verbose) {
                        secretary::typewrite("Scraping...")
                }

                response <-
                tryCatch(
                        xml2::read_html(x = x,
                                        encoding = encoding,
                                        ...,
                                        options = options),
                        error =
                                function(e) NULL
                )

                if (is.null(response)) {

                        if (verbose) {
                                secretary::typewrite("Scraping...failed.")
                                secretary::typewrite("Scraping again...")
                        }

                        Sys.sleep(sleep_time)

                        response <-
                                tryCatch(
                                        xml2::read_html(x = x,
                                                        encoding = encoding,
                                                        ...,
                                                        options = options),
                                        error =
                                                function(e) NULL
                                )

                        secretary::typewrite("Scraping again...complete.")
                } else {

                        secretary::typewrite("Scraping...complete.")
                }

                closeAllConnections()
                response
        }
