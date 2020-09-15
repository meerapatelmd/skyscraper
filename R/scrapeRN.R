



scrapeRN <-
        function(conn,
                 rn_url,
                 sleep_time = 3) {

               #rn_url <-  "https://chem.nlm.nih.gov/chemidplus/rn/9041-08-1"
               #
                if (!missing(conn)) {

                        connSchemas <-
                                pg13::lsSchema(conn = conn)

                        if (!("chemidplus" %in% connSchemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = "chemidplus")

                        }

                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = "chemidplus")

                        if ("CLASSIFICATION" %in% chemiTables) {

                                classification <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = "chemidplus",
                                                                                     tableName = "classifiation",
                                                                                     whereInField = "rn_url",
                                                                                     whereInVector = rn_url))

                        }


                }

                # Proceed if:
                # Connection was provided and no Classificaiton Table exists
                # Connection was provided and classification is nrow 0
                # No connection was provided

                if (!missing(conn)) {
                        if ("CLASSIFICATION" %in% chemiTables) {
                                proceed <- nrow(classification) == 0
                        } else {
                                proceed <- TRUE
                        }
                } else {
                        proceed <- TRUE
                }


                if (proceed) {

                        response <- xml2::read_html(rn_url)
                        Sys.sleep(sleep_time)

                        classifications <-
                               response %>%
                                       rvest::html_nodes("#classifications li") %>%
                                       html_text() %>%
                               tibble::as_tibble_col("concept_classification") %>%
                               dplyr::transmute(scrape_datetime = as.character(Sys.time()),
                                                concept_classification,
                                                rn_url = rn_url) %>%
                               dplyr::distinct()



                        if (!missing(conn)) {


                                if ("CLASSIFICATION" %in% chemiTables) {
                                        pg13::appendTable(conn = conn,
                                                          schema = "chemidplus",
                                                          tableName = "classification",
                                                          classifications)
                                } else {
                                        pg13::writeTable(onn = conn,
                                                         schema = "chemidplus",
                                                         tableName = "classification",
                                                         classifications)
                                }

                        }


                }


                if (!missing(conn)) {

                        connSchemas <-
                                pg13::lsSchema(conn = conn)

                        if (!("chemidplus" %in% connSchemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = "chemidplus")

                        }

                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = "chemidplus")

                        if ("SYNONYMS" %in% chemiTables) {

                                synonyms <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = "chemidplus",
                                                                                     tableName = "synonyms",
                                                                                     whereInField = "rn_url",
                                                                                     whereInVector = rn_url))

                        }


                }



               synonym_types <-
                       response %>%
                       rvest::html_nodes("#names h3") %>%
                       rvest::html_text()



               synonyms_content <-
               response %>%
                       rvest::html_nodes("#names") %>%
                       html_text()


               synonyms_content <-
                       synonyms_content %>%
                       stringr::str_remove_all(pattern = "Names and Synonyms")


               synonyms_content2 <- unlist(strsplit(synonyms_content, split = "[\r\n\t]"))


               synonyms_content3 <-
                       stringr::str_remove_all(synonyms_content2, "[^ -~]")

               synonyms_content4 <- unlist(centipede::strsplit(synonyms_content3, type = "after", split = paste(synonym_types, collapse = "|")))



               index <- list()
               synonym_types2 <- synonym_types
               while (length(synonym_types)) {


                       synonym_type <- synonym_types[1]

                       index[[length(index)+1]] <-
                                 grep(synonym_type,
                                      synonyms_content4)


                       synonym_types <- synonym_types[-1]
               }

               index <- unlist(index)
               ending <- c((index[-1])-1,
                           length(synonyms_content4))

               df <-
               data.frame(index, ending) %>%
                       dplyr::mutate(starting = index+1)

               df$starting %>%
                       purrr::map2(df$ending,
                                   function(x,y) synonyms_content4[x:y]) %>%
                       purrr::set_names(synonym_types2) %>%
                       purrr::map(tibble::as_tibble_col, "concept_synonym_name") %>%
                       dplyr::bind_rows(.id = "concept_synonym_type")


        }
