#' @title
#' Maintain the RN Tables into OMOP Vocabulary Tables
#'
#' @description
#' This function appends the Concept, Concept Synonym, Concept Ancestor, and Concept Relationship Tables from the Classification, Synonyms, and Phrase Log Tables in the chemidplus schema. If the Concept, Concept Synonym, Concept Ancestor, and/or Concept Relationship Tables do not already exist in the chemidplus Schema, an error will be thrown and the setupRNtoOMOP function should be run instead.
#'
#' @param conn Postgres connection
#'
#' @details
#' concept_class_id is the search type. For broader search types such as "contains", there will be the same synonyms matched to different concept ids such as 35101002 & 35100499
#'
#' @seealso
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{readTable}},\code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[purrr]{map}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[rubix]{mutate_all_as_char}},\code{\link[rubix]{normalize_all_to_na}},\code{\link[rubix]{make_identifier}}
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{filter_all}},\code{\link[dplyr]{group_by}}
#'  \code{\link[tidyr]{extract}}
#'  \code{\link[chariot]{queryAthena}},\code{\link[chariot]{filterValid}}
#'  \code{\link[stringr]{str_remove}}
#' @rdname maintainRNtoOMOP
#' @export
#' @importFrom pg13 lsTables readTable query appendTable
#' @importFrom purrr map
#' @importFrom tibble as_tibble
#' @importFrom rubix mutate_all_as_char normalize_all_to_na make_identifier
#' @importFrom dplyr filter mutate select distinct left_join transmute filter_at inner_join group_by ungroup
#' @importFrom tidyr extract
#' @importFrom chariot queryAthena filterValid
#' @importFrom stringr str_remove_all
#' @importFrom magrittr %>%


maintainRNtoOMOP <-
        function(conn,
                 file_report = TRUE,
                 file_report_to = "report.txt",
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
                        report <- paste0("[",as.character(Sys.time()), "]\tmaintainRNtoOMOP")

                        report <-
                              c(report,
                                chemiTableData %>%
                                        purrr::map2(names(chemiTableData),
                                                    function(x,y) paste0(y, ": ", x, " rows")) %>%
                                        unlist()
                              )
                }



                # Pair Phrase_Log Diffs back with concept_id
                # 1. Join the phrase log with the concept table in the chemidplus concept table on the input and type.
                # 2. Filter for any NA concept_ids, meaning that there are new phrase_log observations that need processing
                # If there are new phrases only:
                #       3. All unique input, rn, and type diffs are filtered for any NA rn
                #       4. Join with HemOnc Vocabulary (Concept Table) that are valid, in the Drug domain
                #       5. Create concept_table input and filter out any concept_id == NA
                #       6. Append Concept Table with the diff

                #1.
                phrase_rn <-
                        pg13::query(conn = conn,
                                    sql_statement = "SELECT c.concept_id, pl.input, pl.type, pl.rn
                                                        FROM chemidplus.phrase_log pl
                                                        LEFT JOIN chemidplus.concept c
                                                        ON c.concept_name = pl.input
                                                                AND c.concept_class_id = pl.type
                                                        WHERE pl.no_record = 'FALSE'") %>%
                #2.
                        tibble::as_tibble() %>%
                        rubix::mutate_all_as_char() %>%
                        rubix::normalize_all_to_na() %>%
                        dplyr::filter(is.na(concept_id)) %>%
                        dplyr::mutate(concept_id = as.integer(concept_id))


                if (file_report) {

                        report <-
                                c(report,
                                  paste0(nrow(phrase_rn),
                                         " rows in chemiplus.phrase_log Table that do not join to a chemidplus.concept by input/concept_name and type/concept_class_id"))

                }


                if (nrow(phrase_rn)) {

                        # 3.
                        phrase_rn <-
                                phrase_rn %>%
                                dplyr::select(input,rn,type) %>%
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
                        dplyr::left_join(phrase_rn,
                                         hemonc,
                                         by = c("input" = "concept_name"))


                        # 5.
                        concept_table_diff <-
                                concept_table_diff  %>%
                                dplyr::transmute(
                                                concept_id,
                                                concept_name = input,
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
                                                 " rows appended to chemidplus.concept"))
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
                                                tableName = "synonyms") %>%
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
                                                 " rows appended to chemidplus.concept_synonym"))
                        }

                } else {

                        if (file_report) {
                                report <-
                                        c(report,
                                          "No diff found and zero rows appended to chemidplus.concept_synonym & chemidplus.concept_synonym")
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
                                          paste0(nrow(concept_table_diff), " new chemidplus.classification_concepts added to chemidplus.concept")
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
                                          paste0(nrow(concept_ancestor_table), " new chemidplus.classification_concepts added to chemidplus.concept_ancestor")
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
                                          paste0(nrow(concept_relationship_table), " new chemidplus.classification_concepts added to chemidplus.concept_relationship")
                                        )
                        }




                } else {

                        if (file_report) {
                                report <-
                                        c(report,
                                          "No new chemidplus.classification_concepts found.")
                        }
                }


                if (file_report) {

                        report <-
                                c(report,
                                  "\n")
                        cat(report,
                            file = file_report_to,
                            sep = "\n",
                            append = append_file_report)
                }


        }
