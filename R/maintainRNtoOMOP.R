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
#'  \code{\link[pg13]{readTable}},\code{\link[pg13]{query}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rubix]{mutate_all_as_char}},\code{\link[rubix]{mutate_all_na_str_to_na}},\code{\link[rubix]{make_identifier}}
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{filter_all}},\code{\link[dplyr]{group_by}}
#'  \code{\link[tidyr]{extract}}
#'  \code{\link[chariot]{queryAthena}},\code{\link[chariot]{filterValid}}
#'  \code{\link[stringr]{str_remove}}
#' @rdname setupRNtoOMOP
#' @export
#' @importFrom pg13 readTable query lsTables appendTable writeTable
#' @importFrom rubix mutate_all_as_char mutate_all_na_str_to_na make_identifier
#' @importFrom dplyr filter select distinct left_join transmute mutate filter_at inner_join group_by ungroup
#' @importFrom tidyr extract
#' @importFrom chariot queryAthena filterValid
#' @importFrom stringr str_remove_all
#' @importFrom magrittr %>%


maintainRNtoOMOP <-
        function(conn) {
                #conn <- chariot::connectAthena()


                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = "chemidplus")

                stopifnot(("CONCEPT" %in% chemiTables),
                          ("CONCEPT_SYNONYM" %in% chemiTables),
                          ("CONCEPT_ANCESTOR" %in% chemiTables),
                          ("CONCEPT_RELATIONSHIP" %in% chemiTables))


                chemiTableData <-
                        c("CONCEPT",
                          "CONCEPT_SYNONYM",
                          "CONCEPT_ANCESTOR",
                          "CONCEPT_RELATIONSHIP") %>%
                        purrr::map(~pg13::readTable(conn = conn,
                                                    schema = "chemidplus",
                                                    tableName = .))

                # Pair Phrase_Log Diffs back with concept_id
                # 1. Join the phrase log with the concept table in the chemidplus concept table on the input and type.
                # 2. Filter for any NA concept_ids, meaning that there are new phrase_log observations that need processing
                # If there are new phrases only:
                #       3. All unique input, rn, and type diffs are filtered for any NA rn
                #       4. Join with HemOnc Vocabulary (Concept Table) that are valid, in the Drug domain
                #       5. Create concept_table input and filter out any concept_id == NA
                #       6. Append Table

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
                        dplyr::filter(is.na(concept_id))


                if (nrow(phrase_rn) == 0) {

                } else {
                        # 3.
                        phrase_rn <-
                                phrase_rn %>%
                                dplyr::select(input,rn,type) %>%
                                dplyr::distinct() %>%
                                dplyr::filter(!is.na(rn))

                        # 4. HemOnc concept_ids and concept_name
                        hemonc <-
                        pg13::query(conn = conn,
                                    sql_statement = "SELECT DISTINCT concept_id,concept_name
                                                        FROM public.concept
                                                        WHERE
                                                                vocabulary_id = 'HemOnc'
                                                                        AND invalid_reason IS NULL
                                                                        AND domain_id = 'Drug';")

                        concept_table <-
                        dplyr::left_join(phrase_rn,
                                         hemonc,
                                         by = c("input" = "concept_name"))


                        # 5.
                        concept_table <-
                                concept_table  %>%
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
                                          concept_table)
                }




                # Pair Synonyms back with concept_id
                # 1. Get unique RN to concept_synonym_name and filter out all rn == NA
                # 2. Join with Concept Table
                # 3. Create Concept Synonym Table by selecting distinct concept_id, concept_synonym_name combinations. All NA Concept Ids are filtered out and the language concept id for English (4180186) is added
                # 4. Write Concept Synonym Table if it doesn't already exist, and if not append the existing one

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
                concept_table <-
                        pg13::readTable(conn = conn,
                                        schema = "chemidplus",
                                        tableName = "concept")


                concept_synonym_table <-
                        dplyr::left_join(synonyms_table,
                                         concept_table,
                                         by = c("rn" = "concept_code")) %>%
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
                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = "chemidplus")

                if ("CONCEPT_SYNONYM" %in% chemiTables) {

                        # pg13::appendTable(conn = conn,
                        #                   schema = "chemidplus",
                        #                   tableName = "concept_synonym",
                        #                   concept_synonym_table)

                } else {
                        pg13::writeTable(conn = conn,
                                         schema = "chemidplus",
                                         tableName = "concept_synonym",
                                         concept_synonym_table
                        )
                }





                # Classification Table
                # 1. Get all unique concept classifications and rn combinations
                # 2. Add classification concepts to concept table with New Concept Ids for all unique classifications with concept_class_id as NA, standard_concept == C, concept_code == NA
                # 3. Create Concept Ancestor Table that maps the ancestor of class and its descendants by reading all the new Class concept concept_id and concept_names from the freshly appended concept table
                # 4. Join the new concept_ids with the classification table object from #1 to derive the ancestor concept id
                # 5. Perform a second join on the RN code of the descendant
                # 6. Write Concept Ancestor Table with all min levels of separation as 0 and all max levels of separation as 1 and where both concept id fields are not NA
                # 7. Add maps to relationships to ATC if it exists by first getting all the classification concepts again, joining on all ATC concepts in lowercase and getting the highest level mapping for the new concepts if there is 1 to many mapping.
                # 8. Write Concept Relationship Table after filtering out any concept_ids that are NA

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
                concept_table <-
                        classification_table %>%
                        dplyr::select(concept_classification) %>%
                        dplyr::distinct()

                concept_table$concept_id <-
                        rubix::make_identifier()+1:nrow(concept_table)

                concept_table <-
                        concept_table  %>%
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
                                  concept_table)

                # 3.
                concept_table <-
                        pg13::readTable(conn = conn,
                                        schema = "chemidplus",
                                        tableName = "concept") %>%
                        dplyr::filter(standard_concept == "C") %>%
                        dplyr::select(concept_id, concept_name) %>%
                        dplyr::distinct()

                # 4.
                concept_ancestor_table <-
                        dplyr::left_join(classification_table,
                                         concept_table,
                                         by = c("concept_classification" = "concept_name")) %>%
                        dplyr::transmute(ancestor_concept_id = concept_id,
                                         ancestor_concept_name = concept_classification,
                                         descendant_concept_code = rn)

                #5.
                concept_table <-
                        pg13::readTable(conn = conn,
                                        schema = "chemidplus",
                                        tableName = "concept") %>%
                        dplyr::filter(standard_concept != "C") %>%
                        dplyr::select(concept_id, concept_code) %>%
                        dplyr::distinct()

                concept_ancestor_table <-
                        dplyr::left_join(concept_ancestor_table,
                                         concept_table,
                                         by = c("descendant_concept_code" = "concept_code")) %>%
                        dplyr::transmute(ancestor_concept_id,
                                         descendant_concept_id = concept_id,
                                         min_levels_of_separation = 0,
                                         max_levels_of_separation = 1) %>%
                        dplyr::filter_at(vars(c(ancestor_concept_id,
                                                descendant_concept_id)),
                                         all_vars(!is.na(.)))


                #6.
                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = "chemidplus")

                if ("CONCEPT_ANCESTOR" %in% chemiTables) {

                        # pg13::appendTable(conn = conn,
                        #                   schema = "chemidplus",
                        #                   tableName = "concept_ancestor",
                        #                   concept_ancestor_table)

                } else {
                        pg13::writeTable(conn = conn,
                                         schema = "chemidplus",
                                         tableName = "concept_ancestor",
                                         concept_ancestor_table
                        )
                }


                #7.
                concept_table <-
                        pg13::readTable(conn = conn,
                                        schema = "chemidplus",
                                        tableName = "concept") %>%
                        dplyr::filter(standard_concept == "C")  %>%
                        dplyr::mutate(lower_concept_name = tolower(concept_name)) %>%
                        dplyr::select(concept_id, lower_concept_name)


                atc_classifications <-
                        chariot::queryAthena("SELECT * FROM public.concept WHERE vocabulary_id = 'ATC'") %>%
                        chariot::filterValid(rm_date_fields = FALSE) %>%
                        dplyr::mutate(lower_concept_name = tolower(concept_name)) %>%
                        dplyr::select(concept_id, lower_concept_name, concept_class_id) %>%
                        dplyr::distinct()

                concept_relationship_table <-
                concept_table %>%
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


                chemiTables <- pg13::lsTables(conn = conn,
                                              schema = "chemidplus")

                if ("CONCEPT_RELATIONSHIP" %in% chemiTables) {

                        # pg13::appendTable(conn = conn,
                        #                   schema = "chemidplus",
                        #                   tableName = "concept_relationship",
                        #                   concept_relationship_table)

                } else {
                        pg13::writeTable(conn = conn,
                                         schema = "chemidplus",
                                         tableName = "concept_relationship",
                                         concept_relationship_table
                        )
                }



        }
