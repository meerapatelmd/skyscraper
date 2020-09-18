#' @title
#' ETL the ChemiDPlus Tables to OMOP Vocabulary Architecture
#' @seealso
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{readTable}},\code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[rubix]{map_names_set}},\code{\link[rubix]{mutate_all_as_char}},\code{\link[rubix]{normalize_all_to_na}},\code{\link[rubix]{make_identifier}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{map2}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{filter_all}},\code{\link[dplyr]{group_by}}
#'  \code{\link[tidyr]{extract}}
#'  \code{\link[chariot]{queryAthena}},\code{\link[chariot]{filterValid}}
#'  \code{\link[stringr]{str_remove}}
#' @rdname chemidplus_tables_to_omop
#' @export
#' @importFrom pg13 lsTables readTable query appendTable
#' @importFrom rubix map_names_set mutate_all_as_char normalize_all_to_na make_identifier
#' @importFrom purrr map map2
#' @importFrom tibble as_tibble
#' @importFrom dplyr filter mutate select distinct left_join transmute filter_at inner_join group_by ungroup
#' @importFrom tidyr extract
#' @importFrom chariot queryAthena filterValid
#' @importFrom stringr str_remove_all
#' @importFrom magrittr %>%


chemidplus_tables_to_omop <-
        function(conn,
                 file_report = TRUE,
                 file_report_to = paste0("chemidplus_tables_to_omop_", Sys.Date(),".txt"),
                 append_file_report = TRUE) {

                #conn <- chariot::connectAthena()


                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = "chemidplus")

                stopifnot(("CONCEPT" %in% chemiTables),
                          ("CONCEPT_SYNONYM" %in% chemiTables),
                          ("CONCEPT_ANCESTOR" %in% chemiTables),
                          ("CONCEPT_RELATIONSHIP" %in% chemiTables))


                chemiTableData <-
                        chemiTables %>%
                        rubix::map_names_set(~pg13::readTable(conn = conn,
                                                              schema ="chemidplus",
                                                              tableName = .)) %>%
                        purrr::map(~nrow(.))



                if (file_report) {
                        report <- paste0("[",as.character(Sys.time()), "]")

                        report <-
                              c("#################################",
                                report,
                                "#################################",
                                "ROW_COUNTS[0]",
                                chemiTableData %>%
                                        purrr::map2(names(chemiTableData),
                                                    function(x,y) paste0(y, ": ", x, " rows")) %>%
                                        unlist(),
                                "\n"
                              )
                }



                # Pair registry_number_log Diffs back with concept_id
                # 1. Join the Registry Number Log with the concept table in the chemidplus concept table on the raw_concept and type.
                # 2. Filter for any NA concept_ids, meaning that there are new registry_number_log observations that need processing
                # If there are new phrases only:
                #       3. All unique raw_concept, rn, and type diffs are filtered for any NA rn
                #       4. Join with HemOnc Vocabulary (Concept Table) that are valid, in the Drug domain
                #       5. Create concept_table raw_concept and filter out any concept_id == NA
                #       6. Append Concept Table with the diff

                #1.
                registry_number_log_diff <-
                        pg13::query(conn = conn,
                                    sql_statement = "SELECT c.concept_id, rnl.raw_concept, rnl.type, rnl.rn
                                                        FROM chemidplus.registry_number_log rnl
                                                        LEFT JOIN chemidplus.concept c
                                                        ON c.concept_name = rnl.raw_concept
                                                                AND c.concept_class_id = rnl.type
                                                        WHERE rnl.no_record = 'FALSE'") %>%
                #2.
                        tibble::as_tibble() %>%
                        rubix::mutate_all_as_char() %>%
                        rubix::normalize_all_to_na() %>%
                        dplyr::filter(is.na(concept_id)) %>%
                        dplyr::mutate(concept_id = as.integer(concept_id))


                if (file_report) {

                        report <-
                                c(report,
                                  paste0(nrow(registry_number_log_diff),
                                         " new rows found in the Chemidplus Registry Number Log Table.[1]"))

                }


                if (nrow(registry_number_log_diff)) {

                        # 3.
                        registry_number_log_diff <-
                                registry_number_log_diff %>%
                                dplyr::select(raw_concept,rn,type) %>%
                                dplyr::distinct() %>%
                                dplyr::filter(!is.na(rn))

                        # 4. HemOnc concept_ids and concept_name
                        hemonc <-
                        pg13::query(conn = conn,
                                    sql_statement = "SELECT DISTINCT cs.concept_id,cs.concept_synonym_name AS concept_name
                                                        FROM public.concept c
                                                        LEFT JOIN public.concept_synonym cs
                                                        ON cs.concept_id = c.concept_id
                                                        WHERE
                                                                c.vocabulary_id = 'HemOnc'
                                                                        AND c.invalid_reason IS NULL
                                                                        AND c.domain_id = 'Drug';")

                        concept_table_diff <-
                        dplyr::left_join(registry_number_log_diff,
                                         hemonc,
                                         by = c("raw_concept" = "concept_name"))


                        # 5.
                        concept_table_diff <-
                                concept_table_diff  %>%
                                dplyr::transmute(
                                                concept_id,
                                                concept_name = raw_concept,
                                                domain_id = "Drug",
                                                vocabulary_id = "ChemiDPlus",
                                                concept_class_id = type,
                                                standard_concept= "NA",
                                                concept_code = rn,
                                                valid_start_date = Sys.Date(),
                                                valid_end_date = as.Date("2099-12-31"),
                                                invalid_reason = "NA") %>%
                                dplyr::filter(!is.na(concept_id))

                        # 6.
                        pg13::appendTable(conn = conn,
                                          schema = "chemidplus",
                                          tableName = "concept",
                                          concept_table_diff)

                        if (file_report) {
                                report <-
                                        c(report,
                                          paste0(nrow(concept_table_diff),
                                                 " rows added to the ChemiDPlus Concept Table.[2]"))
                        }





                        # Pair Synonyms back with concept_id
                        # 1. Get unique RN to concept_synonym_name and filter out all rn == NA
                        # 2. Left Join #1. to Concept Table Diff object from step before
                        # 3. Create Concept Synonym Table by selecting distinct concept_id, concept_synonym_name combinations. All NA Concept Ids are filtered out and the language concept id for English (4180186) is added
                        # 4. Concept Synonym Table is appended

                        #1.
                        synonyms_table <-
                                pg13::readTable(conn = conn,
                                                schema = "chemidplus",
                                                tableName = "names_and_synonyms") %>%
                                dplyr::select(rn_url, concept_synonym_name) %>%
                                tidyr::extract(col = rn_url,
                                               into = "rn",
                                               regex = "^.*[/]{1}(.*$)") %>%
                                dplyr::distinct() %>%
                                dplyr::filter(!is.na(rn))

                        #2.

                        concept_synonym_table <-
                                dplyr::left_join(concept_table_diff,
                                                 synonyms_table,
                                                 by = c("concept_code" = "rn")) %>%
                                dplyr::distinct()

                        #3.
                        concept_synonym_table <-
                                concept_synonym_table %>%
                                dplyr::select(concept_id,
                                              concept_synonym_name) %>%
                                dplyr::distinct() %>%
                                dplyr::filter(!is.na(concept_id)) %>%
                                dplyr::mutate(language_concept_id = 4180186)

                        #4.
                        #
                        pg13::appendTable(conn = conn,
                                          schema = "chemidplus",
                                          tableName = "concept_synonym",
                                          concept_synonym_table)

                        if (file_report) {
                                report <-
                                        c(report,
                                          paste0(nrow(concept_synonym_table),
                                                 " rows added to the ChemiDPlus Concept Synonym Table.[3]"))
                        }

                } else {

                        if (file_report) {
                                report <-
                                        c(report,
                                          "0 rows added to the ChemiDPlus Concept Table.[2]",
                                          "0 rows added to the ChemiDPlus Concept Synonym Table.[3]")
                        }

                }



                # Classification Table
                # 1. Get all unique concept classifications and rn combinations
                # 2. Get all classification table differences by joining with the concept table and filtering for na concept_ids
                # If #2 output has rows:
                #       3. The diff is appended to the Concept table with a new Concept Id is added, concept_class_id as NA, standard_concept == C, concept_code == NA
                #       4. To create entries for the Concept Ancestor Table, the Concept Table diff is joined with with the classification table object from #1 to derive the ancestor concept id and rns that will be the descendants
                #       5. Perform a second join on the RN code of the descendant to get the descendant concept id
                #       6. Append Concept Ancestor Table with all min levels of separation as 0 and all max levels of separation as 1 and where both concept id fields are not NA
                #       7. Add maps to relationships to ATC if it exists by first getting all the classification concepts again, joining on all ATC concepts in lowercase and getting the highest level mapping to the concept table diff if there is 1 to many mapping
                #       8. Append Concept Relationship Table after filtering out any concept_ids that are NA

                #1.
                classification_table <-
                        pg13::readTable(conn = conn,
                                        schema = "chemidplus",
                                        tableName = "classification") %>%
                        dplyr::select(concept_classification,
                                      rn_url) %>%
                        tidyr::extract(col = rn_url,
                                       into = "rn",
                                       regex = "^.*[/]{1}(.*$)")

                # 2.
                classification_table_diff <-
                        pg13::query(conn = conn,
                                    sql_statement = "SELECT DISTINCT cl.concept_classification,cl.rn_url,c.concept_id
                                                        FROM chemidplus.classification cl
                                                        LEFT JOIN chemidplus.concept c
                                                        ON cl.concept_classification = c.concept_name
                                                        WHERE c.standard_concept = 'C'") %>%
                        tibble::as_tibble() %>%
                        rubix::mutate_all_as_char() %>%
                        rubix::normalize_all_to_na() %>%
                        dplyr::filter(is.na(concept_id)) %>%
                        dplyr::select(-concept_id) %>%
                        dplyr::distinct() %>%
                        tidyr::extract(col = rn_url,
                                       into = "rn",
                                       regex = "^.*[/]{1}(.*$)")


                if (nrow(classification_table_diff)) {


                        #3
                        classification_table_diff$concept_id <-
                                rubix::make_identifier()+1:nrow(classification_table_diff)

                        concept_table_diff <-
                                classification_table_diff  %>%
                                dplyr::transmute(
                                        concept_id,
                                        concept_name = concept_classification,
                                        domain_id = "Drug",
                                        vocabulary_id = "ChemiDPlus",
                                        concept_class_id = "NA",
                                        standard_concept= "C",
                                        concept_code = "NA",
                                        valid_start_date = Sys.Date(),
                                        valid_end_date = as.Date("2099-12-31"),
                                        invalid_reason = "NA") %>%
                                dplyr::filter(!is.na(concept_id))


                        pg13::appendTable(conn = conn,
                                          schema = "chemidplus",
                                          tableName = "concept",
                                          concept_table_diff)
                        if (file_report) {
                                report <-
                                        c(report,
                                          paste0(nrow(concept_table_diff), " new ChemiDPlus Classification Concepts added to the ChemiDPlus Concept Table.[4]")
                                        )
                        }



                        # 4.
                        concept_ancestor_table <-
                                dplyr::left_join(classification_table_diff,
                                                 concept_table_diff,
                                                 by = c("concept_classification" = "concept_name")) %>%
                                dplyr::transmute(ancestor_concept_id = concept_id,
                                                 ancestor_concept_name = concept_classification,
                                                 descendant_concept_code = rn)

                        #5.
                        nonclass_concept_table <-
                                pg13::readTable(conn = conn,
                                                schema = "chemidplus",
                                                tableName = "concept") %>%
                                dplyr::filter(standard_concept != "C") %>%
                                dplyr::select(concept_id, concept_code) %>%
                                dplyr::distinct()

                        concept_ancestor_table <-
                                dplyr::left_join(concept_ancestor_table,
                                                 nonclass_concept_table,
                                                 by = c("descendant_concept_code" = "concept_code")) %>%
                                dplyr::transmute(ancestor_concept_id,
                                                 descendant_concept_id = concept_id,
                                                 min_levels_of_separation = 0,
                                                 max_levels_of_separation = 1) %>%
                                dplyr::filter_at(vars(c(ancestor_concept_id,
                                                        descendant_concept_id)),
                                                 all_vars(!is.na(.)))

                        pg13::appendTable(conn = conn,
                                          schema = "chemidplus",
                                          tableName = "concept_ancestor",
                                          concept_ancestor_table)


                        if (file_report) {
                                report <-
                                        c(report,
                                          paste0(nrow(concept_ancestor_table), " new ChemiDPlus Classification Concepts added to the ChemiDPlus Concept Ancestor Table.[5]")
                                        )
                        }


                        #7.
                        concept_table_diff <-
                                concept_table_diff %>%
                                dplyr::mutate(lower_concept_name = tolower(concept_name)) %>%
                                dplyr::select(concept_id, lower_concept_name)


                        atc_classifications <-
                                chariot::queryAthena("SELECT * FROM public.concept WHERE vocabulary_id = 'ATC'") %>%
                                chariot::filterValid(rm_date_fields = FALSE) %>%
                                dplyr::mutate(lower_concept_name = tolower(concept_name)) %>%
                                dplyr::select(concept_id, lower_concept_name, concept_class_id) %>%
                                dplyr::distinct()

                        concept_relationship_table <-
                        concept_table_diff %>%
                                dplyr::inner_join(atc_classifications,
                                                 by = "lower_concept_name",
                                                 suffix = c(".chemidplus", ".atc")) %>%
                                dplyr::mutate(atc_level = as.integer(stringr::str_remove_all(concept_class_id, "[^0-9]"))) %>%
                                dplyr::group_by(concept_id.chemidplus) %>%
                                dplyr::filter(atc_level == min(atc_level)) %>%
                                dplyr::ungroup() %>%
                                dplyr::transmute(concept_id_1 = concept_id.chemidplus,
                                                 concept_id_2 = concept_id.atc,
                                                 relationship_id = "Maps to (ATC)",
                                                 valid_start_date = Sys.Date(),
                                                 valid_end_date = as.Date("2099-12-31"),
                                                 invalid_reason = "NA") %>%
                                dplyr::filter_at(vars(c(concept_id_1,
                                                        concept_id_2)),
                                                 all_vars(!is.na(.)))


                        pg13::appendTable(conn = conn,
                                          schema = "chemidplus",
                                          tableName = "concept_relationship",
                                          concept_relationship_table)

                        if (file_report) {
                                report <-
                                        c(report,
                                          paste0(nrow(concept_relationship_table), " new ChemiDPlus Classification Concepts added to the ChemiDPlus Concept Relationship Table.[6]")
                                        )
                        }




                } else {

                        if (file_report) {
                                report <-
                                        c(report,
                                          "0 new ChemiDPlus Classification Concepts added to the ChemiDPlus Concept Table.[4]",
                                          "0 new ChemiDPlus Classification Concepts added to the ChemiDPlus Concept Ancestor Table.[5]",
                                          "0 new ChemiDPlus Classification Concepts added to the ChemiDPlus Concept Relationship Table.[6]")
                        }
                }



                if (file_report) {

                        # Update Row Counts
                        chemiTableData <-
                                chemiTables %>%
                                rubix::map_names_set(~pg13::readTable(conn = conn,
                                                                      schema ="chemidplus",
                                                                      tableName = .)) %>%
                                purrr::map(~nrow(.))

                        report <-
                                c(report,
                                  "\n",
                                  "UPDATED_ROW_COUNTS",
                                  chemiTableData %>%
                                          purrr::map2(names(chemiTableData),
                                                      function(x,y) paste0(y, ": ", x, " rows")) %>%
                                          unlist()
                                )

                        report <-
                                c(report,
                                  "\n",
                                  "[0] row counts of all tables in the ChemiDPlus Schema.",
                                  "[1] Each unique concept name (`raw_concept`) is scraped for the Registry Number and then added to the Registry Number Log Table. The Registry Number Log Table is then joined back to the Chemidplus Concept Table. This is a count for all the `raw_concept` and `type` observations that did not join to a `concept_id` in the ChemiDPlus Concept Table.",
                                  "[2] Each diff in [1] is rejoined with all Valid, Drug Domain HemOnc Concepts in the OMOP Concept Table to map the `raw_concept` to a Concept Id.",
                                  "[3] Diff in [2] is joined to the ChemiDPlus Concept Synonym Table by the Registry Number.",
                                  "[4] Unique Classification Concepts (`classification_concept`) from the ChemiDPlus Classification Table that are not in the ChemiDPlus Concept Table where the Standard Concept is 'C'.",
                                  "[5] Unique Classification Concepts (`classification_concept`) from the ChemiDPlus Classification Table that are not in the ChemiDPlus Concept Ancestor Table where the Standard Concept is 'C'.",
                                  "[6] New Classification Concepts that join with an ATC Classification from the Principle OMOP Concept Table that are added to the ChemiDPlus Concept Relationship Table.",
                                  "#################################",
                                  "\n")
                        cat(report,
                            file = file_report_to,
                            sep = "\n",
                            append = append_file_report)
                }


        }
