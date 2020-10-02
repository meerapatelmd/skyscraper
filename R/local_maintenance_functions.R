#' @title
#' Skyscraper Local Data Maintenance Functions
#'
#' @description
#' Local maintenance functions are meant to be used to export and instantiate/update a local skyscraper schema with the Data Package Repo. This is important for keeping data updated on one machine when there have been additions made to the Remote Data Package Repository.
#'
#' @section
#' Check Schema Status:
#' All the Data Packages in the Schema Map returned by \code{\link{map_schema}} are installed if there are changes to the SHA hash. An "updated" field is added to the Schema Map that contain the values of TRUE if a new installation occurred and FALSE otherwise. If an update to the schema is desired after running this function, \code{\link{update_schemas}}, should be run with `force_update` set to `TRUE`.
#'
#' @section
#' Update Schemas:
#' Checking Schema Status is not a required prerequisite to update skyscraper schemas. However, if the status is checked before running an update, the update needs to be run with force_update set to `TRUE` since the execution of the update occurs on the condition of requiring a new installation, which the \code{\link{schema_status}} would have already executed.
#'
#' @section
#' Exporting Schemas:
#' Exporting schemas requires having a local clone of its Data Package Repository. The tables for the given schema are written to csvs in the "data-raw/", along with a refresh of DATASET.R where the usethis::use_raw_data() function is called. The R/data.R is also rewritten with updates on the dataframe dimensions. The updated local repo is then repackaged and pushed to the remote.
#'
#' @name local_maintenance_functions
NULL

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

