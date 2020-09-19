





export_schema_to_data_repo <-
        function(target_dir,
                 schema) {

                target_dir <- path.expand(target_dir)


                outputPath <- paste0(target_dir, "data-raw/")

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


                outputPaths <- paste0(outputPath, names(Data), ".csv")

                Data %>%
                        purrr::map2(outputPaths,
                                    function(x,y) broca::simply_write_csv(x = x,
                                                                          file = y))


                loadLines <-
                        Tables %>%
                        purrr::map2(basename(outputPaths),
                                    function(x, y) paste0(x, " <- broca::simply_read_csv('", outputPath, y, "')")) %>%
                        unlist()

                # setWdLines <-
                #         paste0("\nsetwd('",dirname(outputPath), "')")

                usethisLines <-
                        paste0("\nusethis::use_data(", paste(Tables, collapse = ","), ", overwrite = TRUE)")

                readr::write_lines(c("library(broca)\n",
                                     loadLines,
                                     #setWdLines,
                                     usethisLines),
                                   path = paste0(outputPath, "DATASET.R"))


                source(paste0(outputPath, "DATASET.R"),
                       local = TRUE)


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
                        cat(file = paste0(target_dir, "/data.R"))


                setwd(target_dir)
                glitter::docPushInstall(commit_message = "automated data refresh", has_vignettes = FALSE)
        }
