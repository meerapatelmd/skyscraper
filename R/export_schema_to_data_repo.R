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
