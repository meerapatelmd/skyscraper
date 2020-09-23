#' @title
#' Update Schemas with a Data Package
#'
#' @description
#' Instantiate or Update any of the Postgres schemas governed by the skyscraper Package with the data found in its corresponding Data Package. The map from the skyscraper schema to its Data Package Repository is stored and maintained by \code{\link{map_schema}}.
#'
#' @param conn                  Postgres connection
#' @param schemas               Character vector of length 1 or more of the schemas to update
#' @param all                   If TRUE, all the skyscraper schemas will be updated and the `schemas` argument is ignored. Default: FALSE
#' @param force_update          If TRUE, the Data Package repository is force-installed and the schema is dropped and repopulated without checking for a difference. Default: FALSE
#'
#' @return
#' Postgres schemas are dropped and repopulated with data found in the corresponding Data Package.
#'
#' @details
#' This function operates on the dataframe returned by \code{\link{map_schema}}. This dataframe is then filtered for the `schemas` argument, if provided, or is otherwise run to completion on all schemas. Since this function cascade drops a schema before repopulating it with package data, the arguments are structured in such a way that a user has to call for a complete refresh of all possible schemas by setting the `all` argument to `TRUE` in order to prevent data loss due to bugs.
#'
#' The Namespace for all the Data Packages are unloaded at the start of execution to prevent the wrong Data Package from being loaded into the wrong schema since the dataframe names are duplicated across packages. Without a forced update, the corresponding Data Package is first installed only if there is a new version of the package detected when installing from GitHub. The fresh install of the Data Package is loaded, all columns with a "datetime" string match are converted to "POSIXct" "POSIXt", and then populates the schema after the present schema is dropped. In a forced update, the corresponding Data Package is force-installed and the schema is dropped and refreshed regardless of whether or not a difference has been detected in the Data Package GitHub repo.
#' @inheritSection  local_maintenance_functions Update Schemas
#'
#' @seealso
#'  \code{\link[rubix]{filter_for}},\code{\link[rubix]{map_names_set}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{map2}}
#'  \code{\link[utils]{capture.output}}
#'  \code{\link[devtools]{remote-reexports}}
#'  \code{\link[pg13]{dropSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{writeTable}}
#'  \code{\link[dplyr]{mutate_all}}
#'  \code{\link[lubridate]{ymd_hms}}
#'
#' @rdname update_schemas
#'
#' @family local maintenance
#'
#' @export
#'
#' @importFrom rubix filter_for map_names_set
#' @importFrom purrr map map2
#' @importFrom utils capture.output
#' @importFrom devtools install_github
#' @importFrom pg13 dropSchema createSchema writeTable
#' @importFrom dplyr mutate_at vars
#' @importFrom lubridate ymd_hms
#' @importFrom magrittr %>%

update_schemas <-
        function(conn,
                 schemas,
                 all = FALSE,
                 force_update = FALSE) {

                # rm(list = ls()[!(ls() %in% c("conn",
                #                              "schema",
                #                              "schema_map",
                #                              "dataPackage",
                #                              "force_update",
                #                              "install_msg"))],
                #    envir = global_env())

                if (missing(schemas) && all == FALSE) {
                        stop('schemas argument required when all == FALSE')
                }

                schema_map <- map_schema()

                if (all) {

                        dataPackages <- schema_map$dataPackage
                        dataPackagesSchema <- schema_map$schema
                        dataPackagesRepo <- schema_map$repo


                } else {

                        current_schema_map <- schema_map %>%
                                                rubix::filter_for(filter_col = "schema",
                                                                  inclusion_vector = schemas)

                        dataPackages <- current_schema_map$dataPackage
                        dataPackagesSchema <- current_schema_map$schema
                        dataPackagesRepo <- current_schema_map$repo

                }

                # Unload Namespaces
                unloadPackages <- schema_map$dataPackage

                unloadPackages %>%
                        purrr::map(~unloadNamespace(.))



                for (i in 1:length(dataPackages)) {

                        dataPackage <- dataPackages[i]
                        dataPackageSchema <- dataPackagesSchema[i]
                        dataPackageRepo <- dataPackagesRepo[i]

                        install_msg <-
                                utils::capture.output(
                                        devtools::install_github(dataPackageRepo),
                                        type = "message",
                                        force = force_update)


                        if (!force_update) {

                                        if (!any(grepl(pattern = "the SHA1.*has not changed since last install",
                                                  x = install_msg))) {


                                                pg13::dropSchema(conn = conn,
                                                                 schema = dataPackageSchema,
                                                                 cascade = TRUE)

                                                pg13::createSchema(conn = conn,
                                                                   schema = dataPackageSchema)

                                                library(dataPackage,
                                                        character.only = TRUE)

                                                DATASETS <- data(package = dataPackage)
                                                DATASETS <- DATASETS$results[, "Item"]


                                                DATA <-
                                                        DATASETS %>%
                                                        rubix::map_names_set(get) %>%
                                                        purrr::map(function(x) x %>%
                                                                   dplyr::mutate_at(dplyr::vars(ends_with("datetime")),
                                                                                    lubridate::ymd_hms))

                                                DATA %>%
                                                        purrr::map2(names(DATA),
                                                                    function(x,y) pg13::writeTable(conn = conn,
                                                                                                   schema = dataPackageSchema,
                                                                                                   tableName = y,
                                                                                                   x))

                                                unloadNamespace(dataPackage)

                                        }
                                } else {

                                        pg13::dropSchema(conn = conn,
                                                         schema = dataPackageSchema,
                                                         cascade = TRUE)


                                        pg13::createSchema(conn = conn,
                                                           schema = dataPackageSchema)


                                        library(dataPackage,
                                                character.only = TRUE)

                                        DATASETS <- data(package = dataPackage)
                                        DATASETS <- DATASETS$results[, "Item"]


                                        DATA <-
                                                DATASETS %>%
                                                rubix::map_names_set(get) %>%
                                                purrr::map(function(x) x %>%
                                                                   dplyr::mutate_at(dplyr::vars(ends_with("datetime")),
                                                                                    lubridate::ymd_hms))

                                        DATA %>%
                                                purrr::map2(names(DATA),
                                                            function(x,y) pg13::writeTable(conn = conn,
                                                                                           schema = schema,
                                                                                           tableName = y,
                                                                                           x))

                                        unloadNamespace(dataPackage)
                                }
                        }
        }


