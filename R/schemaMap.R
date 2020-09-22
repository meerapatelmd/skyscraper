#' @title
#' Map a skyscraper Postgres Schema to a Data Package
#' @seealso
#'  \code{\link[tibble]{tribble}}
#' @rdname schemaMap
#' @export
#' @importFrom tibble tribble


schemaMap <-
        function() {


                tibble::tribble(~schema, ~dataPackage,
                                "cancergov", "cancergovData",
                                "chemidplus_search", "chemidplusSearchData",
                                "chemidplus", "chemidplusData")

        }
