#' @title
#' Get the Status of a Data Package Installation
#'
#' @inherit local_maintenance_functions description
#'
#' @inheritSection local_maintenance_functions Check Schema Status
#'
#' @seealso
#'  \code{\link[rubix]{filter_for}}
#'  \code{\link[purrr]{map}}
#'  \code{\link[utils]{capture.output}}
#'  \code{\link[devtools]{remote-reexports}}
#'
#' @rdname schema_status
#'
#' @family local maintenance
#'
#' @export
#'
#' @importFrom rubix filter_for
#' @importFrom purrr map
#' @importFrom utils capture.output
#' @importFrom devtools install_github
#' @importFrom magrittr %>%

schema_status <-
        function() {

                schema_map <- map_schema()

                if (!missing(schemas)) {

                        schema_map <-
                                schema_map %>%
                                rubix::filter_for(filter_col = schema,
                                                  inclusion_vector = schemas)

                }

                schema_map$updated <-
                        schema_map$repo %>%
                        purrr::map(function(x)
                                utils::capture.output(devtools::install_github(x),
                                                      type = "message")) %>%
                        purrr::map(function(x) !any(grepl(pattern = "the SHA1.*has not changed since last install",
                                                          x))) %>%
                        unlist()

                schema_map



        }

