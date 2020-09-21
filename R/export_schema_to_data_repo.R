#' @title
#' Export a Schema in a Postgres Database to a Repo
#'
#' @description
#' There should be a Data Repo on the local machine that is linked to the schema.
#'
#' @param target_dir Local repo where the schema data will be exported to. If it does not already exist on the local drive, it should be cloned beforehand.
#' @param schema Schema
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


export_schema_to_data_repo <-
        function(target_dir,
                 schema) {
                        # target_dir <- "/Users/meerapatel/GitHub/chemidplusData/"
                        # schema <- "chemidplus"


                        target_dir <- path.expand(target_dir)


                        current_wd <- getwd()
                        setwd(target_dir)

                        data_raw_path <- paste0(target_dir, "/data-raw/")
                        data_path <- paste0(target_dir, "/data/")
                        r_path <-  paste0(target_dir, "/R/")

                        cave::create_dir_if_not_exist(data_raw_path)
                        cave::create_dir_if_not_exist(data_path)
                        cave::create_dir_if_not_exist(r_path)

                        conn <- chariot::connectAthena()
                        Tables <-
                                pg13::lsTables(conn = conn,
                                               schema = schema)
                        Data <<-
                                Tables %>%
                                rubix::map_names_set(~pg13::readTable(conn = conn,
                                                                      schema = schema,
                                                                      tableName = .))
                        chariot::dcAthena(conn = conn)


                        data_raw_paths <- paste0(data_raw_path, "/", names(Data), ".csv")

                        Data %>%
                                purrr::map2(data_raw_paths,
                                            function(x,y) broca::simply_write_csv(x = x,
                                                                                  file = y))


                        loadLines <-
                                Tables %>%
                                purrr::map2(basename(data_raw_paths),
                                            function(x, y) paste0(x, " <- broca::simply_read_csv('", data_raw_path, "/", y, "')")) %>%
                                unlist()

                        # setWdLines <-
                        #         paste0("\nsetwd('",dirname(data_raw_path), "')")

                        usethisLines <-
                                paste0("\nusethis::use_data(", paste(Tables, collapse = ","), ", overwrite = TRUE)")

                        readr::write_lines(c("library(broca)\n",
                                             loadLines,
                                             #setWdLines,
                                             usethisLines),
                                           path = paste0(data_raw_path, "/DATASET.R"))


                        source(paste0(data_raw_path, "/DATASET.R"),
                               local = TRUE)

                        if (interactive()) {

                                current_wd_data_path <- paste0(current_wd, "/data/")


                                data_files <- list.files(current_wd_data_path,
                                                         full.names = TRUE,
                                                         pattern = "[.]{1}rda$") %>%
                                                grep(pattern = paste(Tables, collapse = "|"), value = TRUE)

                                new_location_data_files <-
                                       paste0(data_path, "/", basename(data_files))


                                for (i in 1:length(data_files)) {
                                        file.copy(from = data_files[i],
                                                  to = new_location_data_files[i],
                                                  overwrite = TRUE)

                                        file.remove(data_files[i])
                                }

                                if (!length(list.files(current_wd_data_path))) {
                                        unlink(current_wd_data_path, recursive = TRUE)
                                }

                        }


                        paste0("Data$", names(Data)) %>%
                                purrr::map(sinew::makeOxygen, print = FALSE) %>%
                                purrr::map2(names(Data), function(x,y)
                                        stringr::str_replace(string = x,
                                                             pattern = "(.*\")(.*)(\")",
                                                             replacement = paste0("\\1", y, "\\3"))) %>%
                                purrr::map2(names(Data), function(x,y)
                                        stringr::str_replace(string = x,
                                                             pattern = "DATASET_TITLE",
                                                             replacement = y)) %>%
                                unlist() %>%
                                paste(collapse = "\n\n") %>%
                                cat(file = paste0(r_path, "/data.R"))


                        glitter::docPushInstall(commit_message = "automated data refresh", has_vignettes = FALSE)

                        rm(list = "Data",
                           envir = global_env())

                        if (interactive()) {
                                setwd(current_wd)
                        }

        }
