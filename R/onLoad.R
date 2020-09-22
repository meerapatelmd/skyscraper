#' @title
#' Setup and Update skyscraper Schemas
#' @param schemas (optional) character vector of 1 or more schemas to filter for setup and updating
#' @seealso
#'  \code{\link[rubix]{filter_for}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{map2}}
#'  \code{\link[utils]{capture.output}}
#'  \code{\link[devtools]{remote-reexports}}
#'  \code{\link[dplyr]{filter}}
#' @rdname onLoad
#' @export
#' @importFrom rubix filter_for
#' @importFrom pg13 lsSchema
#' @importFrom purrr map map2
#' @importFrom utils capture.output
#' @importFrom devtools install_github
#' @importFrom dplyr filter
#' @importFrom magrittr %>%

onLoad <-
        function(schemas,
                 conn) {

                schema_map <- schemaMap()

                if (!missing(schemas)) {

                        schema_map <-
                                schema_map %>%
                                rubix::filter_for(filter_col = schema,
                                                  inclusion_vector = schemas)

                }

                schema_map$updated <-
                        schema_map$dataPackage %>%
                        purrr::map(function(x)
                                utils::capture.output(devtools::install_github(paste0("meerapatelmd/",x)),
                                                      type = "message")) %>%
                        purrr::map(function(x) !any(grepl(pattern = "the SHA1.*has not changed since last install",
                                                          x))) %>%
                        unlist()


                schema_map <-
                        schema_map %>%
                        dplyr::filter(updated == TRUE)

                if (nrow(schema_map)) {

                        conn_schemas <- pg13::lsSchema(conn = conn)

                        schema_map$schema %>%
                                purrr::keep(function(x) !(x %in% conn_schemas)) %>%
                                purrr::map2(schema_map$dataPackage,
                                            function(x,y) setupSchema(
                                                    conn = conn,
                                                    schema = x,
                                                    dataPackage = y)
                                )

                        schema_map$schema %>%
                                purrr::keep(function(x) (x %in% conn_schemas)) %>%
                                purrr::map2(schema_map$dataPackage,
                                            function(x,y) updateSchema(conn = conn,
                                                                       schema = x,
                                                                       dataPackage = y))


                }


        }

