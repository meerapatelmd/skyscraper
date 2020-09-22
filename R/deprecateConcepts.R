







deprecateConcepts <-
        function(conn,
                 schema) {


                #conn <- chariot::connectAthena()
                #schema <- "chemidplus_search"

                omopTables <- pg13::lsTables(conn = conn,
                                             schema = schema)


                concept_table_enumerated <-
                        pg13::readTable(conn = conn,
                                        schema = schema,
                                        tableName = "concept") %>%
                        dplyr::mutate(invalid_reason == NA) %>%
                        dplyr::mutate(normalize_concept_name = tolower(concept_name)) %>%
                        dplyr::mutate(normalize_concept_name = stringr::str_replace_all(normalize_concept_name, pattern = "[ ]{2,}", replacement = " ")) %>%
                        dplyr::mutate(normalize_concept_name = trimws(normalize_concept_name, which = "both")) %>%
                        dplyr::select(concept_id,
                                      concept_name,
                                      normalize_concept_name) %>%
                        dplyr::distinct() %>%
                        dplyr::group_by(normalize_concept_name) %>%
                        dplyr::mutate(counts = 1:n()) %>%
                        dplyr::mutate(total_counts = n()) %>%
                        dplyr::ungroup()

                total_unique_concepts <-
                        length(unique(concept_table_enumerated$normalize_concept_name))


                part_1 <-
                        concept_table_enumerated %>%
                        dplyr::filter(total_counts == 1)

                part_2 <-
                        concept_table_enumerated %>%
                        dplyr::filter(total_counts != 1)

                part_2_a <-
                        part_2 %>%
                        dplyr::filter(counts == 1) %>%
                        dplyr::select(concept_id,
                                      concept_name,
                                      normalize_concept_name)

                part_2_b <-
                        part_2 %>%
                        dplyr::filter(counts != 1) %>%
                        dplyr::select(normalize_concept_name,
                                      deprecated_concept_id = concept_id)


                system_time <- Sys.time()
                final <-
                        part_2_a %>%
                        dplyr::full_join(part_2_b,  by = "normalize_concept_name") %>%
                        dplyr::distinct() %>%
                        dplyr::transmute(cd_datetime = system_time,
                                         principle_concept_id = concept_id,
                                         concept_name,
                                         deprecated_concept_id)



                if ("CONCEPT_DEPRECATION" %in% omopTables) {
                        pg13::appendTable(conn = conn,
                                         schema = schema,
                                         tableName = "concept_deprecation",
                                         final)
                } else {
                        pg13::writeTable(conn = conn,
                                         schema = schema,
                                         tableName = "concept_deprecation",
                                         final)
                }

                if ("CONCEPT_SYNONYM" %in% omopTables) {

                        new_concept_synonym_table <-
                        pg13::query(conn = conn,
                                    sql_statement = pg13::buildJoinQuery(joinType = "INNER",
                                                                        schema = schema,
                                                                         tableName = "CONCEPT_SYNONYM",
                                                                         column = "concept_id",
                                                                         joinOnSchema = schema,
                                                                         joinOnTableName = "concept_deprecation",
                                                                         joinOnColumn = "deprecated_concept_id"))  %>%
                                dplyr::transmute(concept_id = dplyr::coalesce(principle_concept_id, concept_id),
                                                 concept_synonym_name,
                                                 language_concept_id) %>%
                                dplyr::distinct()


                        pg13::appendTable(conn = conn,
                                         schema = schema,
                                         tableName = "CONCEPT_SYNONYM",
                                         new_concept_synonym_table)


                }

                if ("CONCEPT_ANCESTOR" %in% omopTables) {
                        new_table <-
                                pg13::query(conn = conn,
                                            sql_statement = pg13::buildJoinQuery(joinType = "INNER",
                                                                                 schema = schema,
                                                                                 tableName = "CONCEPT_ANCESTOR",
                                                                                 column = "ancestor_concept_id",
                                                                                 joinOnSchema = schema,
                                                                                 joinOnTableName = "concept_deprecation",
                                                                                 joinOnColumn = "deprecated_concept_id"))  %>%
                                dplyr::transmute(ancestor_concept_id = dplyr::coalesce(principle_concept_id, ancestor_concept_id),
                                                 descendant_concept_id,
                                                 min_levels_of_separation,
                                                 max_levels_of_separation) %>%
                                dplyr::distinct()

                        pg13::appendTable(conn = conn,
                                         schema = schema,
                                         tableName = "CONCEPT_ANCESTOR",
                                         new_table)


                        new_table2 <-
                                pg13::query(conn = conn,
                                            sql_statement = pg13::buildJoinQuery(joinType = "INNER",
                                                                                 schema = schema,
                                                                                 tableName = "CONCEPT_ANCESTOR",
                                                                                 column = "descendant_concept_id",
                                                                                 joinOnSchema = schema,
                                                                                 joinOnTableName = "concept_deprecation",
                                                                                 joinOnColumn = "deprecated_concept_id"))  %>%
                                dplyr::transmute(ancestor_concept_id,
                                                 descendant_concept_id = dplyr::coalesce(principle_concept_id, descendant_concept_id),
                                                 min_levels_of_separation,
                                                 max_levels_of_separation) %>%
                                dplyr::distinct()

                        pg13::appendTable(conn = conn,
                                         schema = schema,
                                         tableName = "CONCEPT_ANCESTOR",
                                         new_table2)
                }


                if ("CONCEPT_RELATIONSHIP" %in% omopTables) {

                        new_table <-
                                pg13::query(conn = conn,
                                            sql_statement = pg13::buildJoinQuery(joinType = "INNER",
                                                                                schema = schema,
                                                                                 tableName = "CONCEPT_RELATIONSHIP",
                                                                                 column = "concept_id_1",
                                                                                 joinOnSchema = schema,
                                                                                 joinOnTableName = "concept_deprecation",
                                                                                 joinOnColumn = "deprecated_concept_id"))  %>%
                                dplyr::transmute(concept_id_1 = dplyr::coalesce(principle_concept_id, concept_id_1),
                                                 concept_id_2,
                                                 relationship_id,
                                                 valid_start_date,
                                                 valid_end_date,
                                                 invalid_reason) %>%
                                dplyr::distinct()

                        pg13::appendTable(conn = conn,
                                         schema = schema,
                                         tableName = "CONCEPT_RELATIONSHIP",
                                         new_table)


                        new_table2 <-
                                pg13::query(conn = conn,
                                            sql_statement = pg13::buildJoinQuery(joinType = "INNER",
                                                                                 schema = schema,
                                                                                 tableName = "CONCEPT_RELATIONSHIP",
                                                                                 column = "concept_id_2",
                                                                                 joinOnSchema = schema,
                                                                                 joinOnTableName = "concept_deprecation",
                                                                                 joinOnColumn = "deprecated_concept_id"))  %>%
                                dplyr::transmute(concept_id_1,
                                                 concept_id_2 = dplyr::coalesce(principle_concept_id, concept_id_2),
                                                 relationship_id,
                                                 valid_start_date,
                                                 valid_end_date,
                                                 invalid_reason) %>%
                                dplyr::distinct()

                        pg13::appendTable(conn = conn,
                                         schema = schema,
                                         tableName = "CONCEPT_RELATIONSHIP",
                                         new_table2)

                }


                updated_concept_table <-
                        pg13::query(conn = conn,
                                    sql_statement = pg13::buildJoinQuery(fields = c(paste0(schema, ".concept.*"),
                                                                                    paste0(schema, ".concept_deprecation.cd_datetime"),
                                                                                    paste0(schema, ".concept_deprecation.deprecated_concept_id")),
                                                                         schema = schema,
                                                                         tableName = "concept",
                                                                         column = "concept_id",
                                                                         joinOnSchema = schema,
                                                                         joinOnTableName = "concept_deprecation",
                                                                         joinOnColumn = "deprecated_concept_id")) %>%
                        dplyr::mutate(new_invalid_reason = ifelse(!is.na(cd_datetime), "D", NA)) %>%
                        dplyr::mutate(invalid_reason = dplyr::coalesce(new_invalid_reason, invalid_reason)) %>%
                        dplyr::select(-cd_datetime,
                                      -deprecated_concept_id,
                                      -new_invalid_reason) %>%
                        dplyr::distinct()


                pg13::renameTable(conn = conn,
                                  schema = schema,
                                  tableName = "concept",
                                  newTableName = "saved_concept")

                pg13::writeTable(conn = conn,
                                 schema = schema,
                                 tableName = "concept",
                                 updated_concept_table)

                pg13::dropTable(conn = conn,
                                schema = schema,
                                tableName = "saved_concept")


        }
