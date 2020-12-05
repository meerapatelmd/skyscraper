#' Pipe operator
#'
#' See \code{magrittr::\link[magrittr:pipe]{\%>\%}} for details.
#'
#' @name %>%
#' @rdname pipe
#' @keywords internal
#' @export
#' @importFrom magrittr %>%
#' @usage lhs \%>\% rhs
NULL



#' @noRd

no_na <-
        function(vector) {
                vector[!is.na(vector)]
        }


#' @noRd

make_identifier <-
        function(digits.secs = 7) {

                op <- options(digits.secs = digits.secs)
                on.exit(options(op),
                        add = TRUE,
                        after = TRUE)

                id <- as.character(Sys.time())
                id <- stringr::str_remove_all(id, pattern = "[^0-9]")
                id <- stringr::str_remove_all(id, pattern = "^202")
                as.double(id)


        }
