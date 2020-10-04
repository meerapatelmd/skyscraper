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
#' Map a skyscraper Schema to a Data Package Repository
#'
#' @description
#' Each Postgres schema governed by this package has a corresponding Data Package, where updates to the Postgres schema are exported as csvs, packaged, and pushed to a GitHub repo.
#'
#' @param repo_username         Github username to which the Data Package belongs, Default: 'meerapatelmd'
#' @seealso
#'  \code{\link[tibble]{tribble}}
#'  \code{\link[dplyr]{mutate}}
#' @rdname map_schema
#' @family local maintenance
#' @export
#' @importFrom tibble tribble
#' @importFrom dplyr mutate
#' @importFrom magrittr %>%


map_schema <-
        function(repo_username = "meerapatelmd") {


                tibble::tribble(~schema, ~dataPackage, ~tables,
                                "cancergov", "cancergovData", "c('DRUG_DICTIONARY', 'DRUG_DICTIONARY_LOG', 'DRUG_LINK', 'DRUG_LINK_SYNONYM', 'DRUG_LINK_URL', 'DRUG_LINK_NCIT')",
                                "chemidplus_search", "chemidplusSearchData", "c('CLASSIFICATION', 'LINKS_TO_RESOURCES', 'NAMES_AND_SYNONYMS', 'REGISTRY_NUMBER_LOG', 'REGISTRY_NUMBERS', 'RN_URL_VALIDITY')",
                                "chemidplus", "chemidplusData", "c('CLASSIFICATION', 'LINKS_TO_RESOURCES', 'NAMES_AND_SYNONYMS', 'REGISTRY_NUMBER_LOG', 'REGISTRY_NUMBERS', 'RN_URL_VALIDITY')",
                                "pubmed_search", "pubmedSearchData", NA) %>%
                        dplyr::mutate(repo = paste0(repo_username, "/", dataPackage))

        }







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




#' @title
#' Export and Push a skyscraper Schema to its Data Package Repository
#'
#' @inherit local_maintenance_functions description
#'
#' @inheritSection local_maintenance_functions Exporting Schemas
#'
#' @param conn          Postgres connection for `schema` argument
#' @param schema        Schema to export
#' @param target_dir    Path to the local repo where the schema data will be exported to. If it does not already exist locally, it should be cloned beforehand.
#'
#' @seealso
#'  \code{\link[cave]{create_dir_if_not_exist}}
#'  \code{\link[chariot]{connectAthena}},\code{\link[chariot]{dcAthena}}
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{readTable}}
#'  \code{\link[rubix]{map_names_set}}
#'  \code{\link[purrr]{map2}},\code{\link[purrr]{map}}
#'  \code{\link[broca]{simply_write_csv}}
#'  \code{\link[readr]{read_lines}}
#'  \code{\link[sinew]{makeOxygen}}
#'  \code{\link[stringr]{str_replace}}
#'  \code{\link[glitter]{docPushInstall}}
#'
#' @rdname export_schema_to_data_repo
#'
#' @family local maintenance
#'
#' @export
#' @importFrom cave create_dir_if_not_exist
#' @importFrom chariot connectAthena dcAthena
#' @importFrom pg13 lsTables readTable
#' @importFrom rubix map_names_set
#' @importFrom purrr map2 map
#' @importFrom broca simply_write_csv
#' @importFrom readr write_lines
#' @importFrom sinew makeOxygen
#' @importFrom stringr str_replace
#' @importFrom glitter docPushInstall
#' @importFrom magrittr %>%
#' @importFrom rlang parse_expr


export_schema_to_data_repo <-
        function(conn,
                 schema,
                 commit_message = "automated export",
                 target_dir,
                 force_install = FALSE,
                 reset = TRUE) {


                # target_dir <- "/Users/meerapatel/GitHub/chemidplusData/"

                # conn <- chariot::connectAthena()
                # schema <- "cancergov"

                # Load Schema Map
                schema_map <- map_schema()

                # Create the dataPackage and
                dataPackage <- unlist((schema_map[schema_map$schema == schema, "dataPackage"]))
                tables <- eval(rlang::parse_expr(unlist((schema_map[schema_map$schema == schema, "tables"]))))
                repo <- unlist((schema_map[schema_map$schema == schema, "repo"]))


                # If the target_dir is missing, it is found in the ~/GitHub directory. If it is not found in the ~/GitHub directory, it is cloned.

                if (missing(target_dir)) {

                        target_dir <-
                                system(paste0("find ~/GitHub -name ", dataPackage),
                                       intern = TRUE)

                        if (length(target_dir) != 1)  {

                                clone_url <-
                                        glitter::get_remote_repos("meerapatelmd") %>%
                                        purrr::pluck("REPOS") %>%
                                        dplyr::filter(name == dataPackage) %>%
                                        dplyr::select(clone_url) %>%
                                        unlist()


                                glitter::clone(remote_url = clone_url,
                                               destination_path = "~/GitHub")

                                target_dir <-
                                        system(paste0("find ~/GitHub -name ", dataPackage),
                                               intern = TRUE)

                                System.sleep(5)

                        }


                }


                target_dir <- path.expand(target_dir)

                data_raw_path <- file.path(target_dir, "data-raw")
                data_path <- file.path(target_dir, "data")
                r_path <-  file.path(target_dir, "R")
                path_to_DATASET <- file.path(data_raw_path, "DATASET.R")
                path_to_dataR <- file.path(r_path, "data.R")

                cave::create_dir_if_not_exist(data_raw_path)
                cave::create_dir_if_not_exist(data_path)
                cave::create_dir_if_not_exist(r_path)


                #current_wd <- getwd()
                #setwd(target_dir)


                # Unload Namespaces prior to installing the package
                schema_map$dataPackage %>%
                        purrr::map(~unloadNamespace(.))


                # Install Data Package to UNION with current schema data
                devtools::install_github(repo)

                Sys.sleep(5)

                library(dataPackage,
                        character.only = TRUE)

                # Getting package datasets name
                DATASETS <- data(package = dataPackage, envir = environment())
                DATASETS <- DATASETS$results[, "Item"]

                # Load package datasets to merge with the local datasets
                importedData <-
                        tables %>%
                        rubix::map_names_set(~get(., envir = environment())) %>%
                        purrr::map(function(x) x %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::ends_with("datetime")),
                                                            lubridate::ymd_hms) %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::contains("concept_id")),
                                                            as.integer) %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::contains("levels_of_separation")),
                                                            as.integer) %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::matches("valid_start_date"),
                                                                        tidyselect::matches("valid_end_date")),
                                                            as.Date) %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::matches("response_received"),
                                                                        tidyselect::matches("no_record"),
                                                                        tidyselect::matches("response_recorded"),
                                                                        tidyselect::matches("is_404")),
                                                            as.character)
                        )

                ############
                ## Local Datasets
                ## ##########

                localData <-
                        pg13::lsTables(conn = conn, schema = schema)  %>%
                        rubix::map_names_set(~pg13::readTable(conn = conn,
                                                              schema = schema,
                                                              tableName = .)) %>%
                        purrr::map(function(x) x %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::ends_with("datetime")),
                                                            lubridate::ymd_hms) %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::contains("concept_id")),
                                                            as.integer) %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::contains("levels_of_separation")),
                                                            as.integer) %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::matches("valid_start_date"),
                                                                        tidyselect::matches("valid_end_date")),
                                                            as.Date) %>%
                                           dplyr::mutate_at(dplyr::vars(tidyselect::matches("response_received"),
                                                                        tidyselect::matches("no_record"),
                                                                        tidyselect::matches("response_recorded"),
                                                                        tidyselect::matches("is_404")),
                                                            as.character)
                        )

                ##############
                #### Merge Local with Imported Data
                ##############

                mergedData <- list()

                for (i in 1:length(tables)) {

                        mergedData[[i]] <-
                                dplyr::bind_rows(importedData[[tables[i]]],
                                                 localData[[tables[i]]])

                        names(mergedData)[i] <- tables[i]

                }

                # Dedupe Merged Data
                # All dataframes with a datetime are deduped and then grouped on all other columns and filtered or the earliest entry
                # If a datetime column is not present, the dataframe is deduped only
                mergedData2 <<-
                        mergedData %>%
                        rubix::map_names_set(function(x) if (any(grepl("datetime", colnames(x)))) {

                                x %>%
                                        dplyr::distinct() %>%
                                        dplyr::group_by_at(dplyr::vars(!contains("datetime"))) %>%
                                        dplyr::arrange_at(dplyr::vars(contains("datetime")), .by_group = TRUE) %>%
                                        rubix::filter_first_row() %>%
                                        dplyr::ungroup()

                        } else {

                                x %>%
                                        dplyr::distinct()

                        }
                        )


                ######
                ### Pull Repo
                ######
                glitter::pull(path_to_local_repo = target_dir,
                              verbose = FALSE)


                ######
                ### Write to output file paths to data-raw folder
                ######
                data_raw_paths <- file.path(data_raw_path, paste0(names(mergedData2), ".csv"))

                mergedData2 %>%
                        purrr::map2(data_raw_paths,
                                    function(x,y) broca::simply_write_csv(x = x,
                                                                          file = y))
                ######
                ### Write DATASETS.R File
                ######

                declareObjsLines <-
                        names(mergedData2) %>%
                        purrr::map2(data_raw_paths,
                                    function(x, y) paste0(x, " <- readr::read_csv('",y, "')")) %>%
                        unlist()

                usethisLines <-
                        paste0("\nusethis::use_data(\n", paste(paste0("\t", names(mergedData2)), collapse = ",\n"), "\n, overwrite = TRUE)")


                cat(c("library(readr)",
                      declareObjsLines,
                      usethisLines),
                    sep = "\n",
                    file = path_to_DATASET)



                # if (interactive()) {
                #
                #         current_wd_data_path <- paste0(current_wd, "/data/")
                #
                #
                #         data_files <- list.files(current_wd_data_path,
                #                                  full.names = TRUE,
                #                                  pattern = "[.]{1}rda$") %>%
                #                         grep(pattern = paste(Tables, collapse = "|"), value = TRUE)
                #
                #         new_location_data_files <-
                #                paste0(data_path, "/", basename(data_files))
                #
                #
                #         for (i in 1:length(data_files)) {
                #                 file.copy(from = data_files[i],
                #                           to = new_location_data_files[i],
                #                           overwrite = TRUE)
                #
                #                 file.remove(data_files[i])
                #         }
                #
                #         if (!length(list.files(current_wd_data_path))) {
                #                 unlink(current_wd_data_path, recursive = TRUE)
                #         }
                #
                # }
                #
                #
                paste0("mergedData2$", names(mergedData2)) %>%
                        purrr::map(sinew::makeOxygen, print = FALSE) %>%
                        purrr::map2(names(mergedData2), function(x,y)
                                stringr::str_replace(string = x,
                                                     pattern = "(.*\")(.*)(\")",
                                                     replacement = paste0("\\1", y, "\\3"))) %>%
                        purrr::map2(names(mergedData2), function(x,y)
                                stringr::str_replace(string = x,
                                                     pattern = "DATASET_TITLE",
                                                     replacement = y)) %>%
                        unlist() %>%
                        paste(collapse = "\n\n") %>%
                        cat(file = path_to_dataR)




                current_wd <- getwd()

                setwd(target_dir)
                source(path_to_DATASET,
                       local = TRUE)
                glitter::docPushInstall(commit_message = "update data package",
                                        has_vignettes = FALSE)

                rm(list = "mergedData2",
                   envir = global_env())

                setwd(current_wd)

                if (reset) {
                        cave::reset()
                }

        }


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
#' @importFrom rlang parse_expr

import_schemas <-
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

                # Unload All Namespaces
                unloadPackages <- schema_map$dataPackage
                unloadPackages %>%
                        purrr::map(~unloadNamespace(.))


                if (!all) {

                        schema_map <- schema_map %>%
                                rubix::filter_for(filter_col = "schema",
                                                  inclusion_vector = schemas)

                }

                dataPackages <- schema_map$dataPackage
                dataPackagesSchema <- schema_map$schema
                dataPackagesRepo <- schema_map$repo
                dataPackagesTables <- schema_map$tables



                for (i in 1:length(dataPackages)) {

                        dataPackage <- dataPackages[i]
                        dataPackageSchema <- dataPackagesSchema[i]
                        dataPackageRepo <- dataPackagesRepo[i]
                        dataPackageTables <- eval(rlang::parse_expr(dataPackageTables[i]))


                        install_msg <- devtools::install_github(dataPackageRepo, force = TRUE, quiet = TRUE)

                        secretary::typewrite("Dropping and importing", secretary::italicize(dataPackageSchema),"...")

                        library(dataPackage,
                                character.only = TRUE)
                        data(package = dataPackage, envir = environment())



                        importedData <-
                                dataPackageTables %>%
                                        rubix::map_names_set(~get(., envir = environment())) %>%
                                        purrr::map(function(x) x %>%
                                                           dplyr::mutate_at(dplyr::vars(ends_with("datetime")),
                                                                            lubridate::ymd_hms))

                        localData <-
                                dataPackageTables %>%
                                rubix::map_names_set(~pg13::readTable(conn = conn,
                                                                      schema = dataPackageSchema,
                                                                      tableName = .))


                        ##############
                        #### Merge Local with Imported Data
                        ##############

                        mergedData <- list()
                        for (j in 1:length(dataPackageTables)) {

                                mergedData[[j]] <-
                                        dplyr::bind_rows(importedData[[dataPackageTables[j]]],
                                                         localData[[dataPackageTables[j]]])

                                names(mergedData)[j] <- dataPackageTables[j]
                        }


                        # Dedupe Merged Data
                        # All dataframes with a datetime are deduped and then grouped on all other columns and filtered or the earliest entry
                        # If a datetime column is not present, the dataframe is deduped only
                        mergedData2 <-
                                mergedData %>%
                                rubix::map_names_set(function(x) if (any(grepl("datetime", colnames(x)))) {

                                                x %>%
                                                        dplyr::distinct() %>%
                                                        dplyr::group_by_at(vars(!contains("datetime"))) %>%
                                                        dplyr::arrange_at(vars(contains("datetime")), .by_group = TRUE) %>%
                                                        rubix::filter_first_row() %>%
                                                        dplyr::ungroup()

                                        } else {

                                                x %>%
                                                        dplyr::distinct()

                                        }

                                )

                        secretary::typewrite("Dropping", secretary::italicize(dataPackageSchema),"...")
                        Sys.sleep(5)

                        pg13::dropSchema(conn = conn,
                                         schema = dataPackageSchema,
                                         cascade = TRUE)


                        Sys.sleep(10)

                        pg13::createSchema(conn = conn,
                                           schema = dataPackageSchema)

                        Sys.sleep(5)

                        secretary::typewrite("Importing", secretary::italicize(dataPackageSchema),"...")

                        for (j in 1:length(mergedData2)) {


                                pg13::writeTable(conn = conn,
                                                 schema = dataPackageSchema,
                                                 tableName = names(mergedData2)[j],
                                                 mergedData2[[j]])

                                Sys.sleep(10)


                        }
                }
        }




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

                .Deprecated("import_schemas")

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
                                                                                   schema = dataPackageSchema,
                                                                                   tableName = y,
                                                                                   x))

                                unloadNamespace(dataPackage)
                        }
                }
        }
