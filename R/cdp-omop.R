#' @title
#' ETL the ChemiDPlus Tables to OMOP Vocabulary Architecture
#'
#' @param destination_schema Schema where the new tables will be written to. User must have write privileges for this schema
#' @param vocab_schema Schema in the connection that houses the OMOP Proper Vocabularies. Cannot be the same as the `destination_schema` because the OMOP Proper Vocabularies would be appended with the ChemiDPlus transformations and it needs to be silo'd until the concepts are vetted.
#' @seealso
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{readTable}},\code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[rubix]{map_names_set}}, \code{\link[rubix]{normalize_all_to_na}},
#'  \code{\link[purrr]{map}},\code{\link[purrr]{map2}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{filter_all}},\code{\link[dplyr]{group_by}}
#'  \code{\link[tidyr]{extract}}
#'  \code{\link[chariot]{queryAthena}},\code{\link[chariot]{filterValid}}
#'  \code{\link[stringr]{str_remove}}
#' @rdname cdp_to_omop
#' @export
#' @importFrom pg13 lsTables readTable query appendTable
#' @importFrom rubix map_names_set normalize_all_to_na
#' @importFrom purrr map map2
#' @importFrom tibble as_tibble
#' @importFrom dplyr filter mutate select distinct left_join transmute filter_at inner_join group_by ungroup mutate_all
#' @importFrom tidyr extract
#' @importFrom chariot queryAthena filterValid
#' @importFrom stringr str_remove_all
#' @importFrom magrittr %>%


cdp_to_omop <-
        function(conn,
                 destination_schema,
                 vocab_schema,
                 verbose = TRUE,
                 render_sql = TRUE) {

                stopifnot(tolower(destination_schema) != tolower(vocab_schema))

                pg13::send(conn = conn,
                           SqlRender::render(
                           "CREATE TABLE IF NOT EXISTS @destination_schema.concept (
                            concept_id bigint,
                            concept_name character varying(255),
                            domain_id character varying(255),
                            vocabulary_id character varying(255),
                            concept_class_id character varying(255),
                            standard_concept character varying(255),
                            concept_code character varying(255),
                            valid_start_date date,
                            valid_end_date date,
                            invalid_reason character varying(255)
                        );


                        CREATE TABLE IF NOT EXISTS @destination_schema.concept_ancestor (
                            ancestor_concept_id bigint,
                            descendant_concept_id bigint,
                            min_levels_of_separation bigint,
                            max_levels_of_separation bigint
                        );

                        CREATE TABLE IF NOT EXISTS @destination_schema.concept_relationship (
                            concept_id_1 bigint,
                            concept_id_2 bigint,
                            relationship_id character varying(255),
                            valid_start_date date,
                            valid_end_date date,
                            invalid_reason character varying(255)
                        );


                        CREATE TABLE IF NOT EXISTS @destination_schema.concept_synonym (
                            concept_id bigint,
                            concept_synonym_name character varying(255),
                            language_concept_id bigint
                        );
                        ",
                           destination_schema = destination_schema),
                           verbose = verbose,
                           render_sql = render_sql)


                chemiTableData <-
                        read_cdp_tables(conn = conn,
                                        verbose = verbose,
                                        render_sql = render_sql)



                if (verbose) {

                        secretary::typewrite(secretary::enbold("Row Counts "))
                        table_nms <- names(chemiTableData)
                        table_rows <-
                                chemiTableData %>%
                                        purrr::map(~ nrow(.)) %>%
                                        purrr::set_names(table_nms)

                        cat(mapply(sprintf, "\t\t\t\t%s: %s\n", table_nms, table_rows))
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
                                    sql_statement = SqlRender::render("SELECT DISTINCT
                                                                                rnl.raw_search_term,
                                                                                rnl.search_type,
                                                                                rnl.rn
                                                                        FROM chemidplus.registry_number_log rnl
                                                                        LEFT JOIN @destination_schema.concept c
                                                                        ON c.concept_name = rnl.raw_search_term
                                                                                AND c.concept_class_id = rnl.search_type
                                                                         WHERE rnl.no_record = 'FALSE'
                                                                                AND c.concept_id IS NULL",
                                                                      destination_schema = destination_schema)) %>%
                #2.
                        tibble::as_tibble()


                if (verbose) {

                        secretary::typewrite(nrow(registry_number_log_diff), "new rows found in the Chemidplus Registry Number Log Table")

                }


                if (nrow(registry_number_log_diff) > 0) {

                        # 3.
                        registry_number_log_diff <-
                                registry_number_log_diff %>%
                                dplyr::distinct() %>%
                                dplyr::filter(!is.na(rn))

                        # 4. HemOnc + RxNorm Ingredient concept_ids and concept_name
                        omop_proper <-
                        pg13::query(conn = conn,
                                    sql_statement = SqlRender::render(
                                                        "
                                                        WITH hemonc AS (
                                                        SELECT DISTINCT cs.concept_id,cs.concept_synonym_name AS concept_name
                                                        FROM @vocab_schema.concept c
                                                        LEFT JOIN @vocab_schema.concept_synonym cs
                                                        ON cs.concept_id = c.concept_id
                                                        WHERE
                                                                c.vocabulary_id = 'HemOnc'
                                                                        AND c.invalid_reason IS NULL
                                                                        AND c.domain_id = 'Drug'
                                                                        AND  cs.language_concept_id = 4180186
                                                        ),
                                                        rxnorm AS (
                                                        SELECT DISTINCT cs.concept_id, cs.concept_synonym_name AS concept_name
                                                 FROM @vocab_schema.concept_ancestor ca
                                                 INNER JOIN @vocab_schema.concept c
                                                 ON c.concept_id = ca.descendant_concept_id
                                                 LEFT JOIN @vocab_schema.concept_synonym cs
                                                 ON cs.concept_id = c.concept_id
                                                 WHERE ca.ancestor_concept_id = 21601386
                                                 AND c.invalid_reason IS NULL
                                                 AND c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
                                                 AND c.concept_class_id IN ('Ingredient', 'Precise Ingredient')
                                                 AND cs.language_concept_id = 4180186
                                                        )

                                                        SELECT *
                                                        FROM hemonc
                                                        UNION
                                                        SELECT *
                                                        FROM rxnorm;
                                                        ",
                                                        vocab_schema = vocab_schema))


                        concept_table_diff <-
                        dplyr::inner_join(registry_number_log_diff,
                                         omop_proper,
                                         by = c("raw_search_term" = "concept_name")) %>%
                                dplyr::distinct()


                        # 5.
                        concept_table_diff <-
                                concept_table_diff  %>%
                                dplyr::transmute(
                                                concept_id,
                                                concept_name = raw_search_term,
                                                domain_id = "Drug",
                                                vocabulary_id = "ChemiDPlus",
                                                concept_class_id = search_type,
                                                standard_concept= "NA",
                                                concept_code = rn,
                                                valid_start_date = Sys.Date(),
                                                valid_end_date = as.Date("2099-12-31"),
                                                invalid_reason = "NA") %>%
                                dplyr::filter(!is.na(concept_id)) %>%
                                dplyr::distinct()

                        # 6.
                        pg13::appendTable(conn = conn,
                                          schema = destination_schema,
                                          tableName = "concept",
                                          data = concept_table_diff)

                        if (verbose) {
                                secretary::typewrite(nrow(concept_table_diff), " rows added to the ChemiDPlus Concept Table.")
                        }





                        # Pair Synonyms back with concept_id
                        # 1. Get unique RN to concept_synonym_name and filter out all rn == NA
                        # 2. Left Join #1. to Concept Table Diff object from step before
                        # 3. Create Concept Synonym Table by selecting distinct concept_id, concept_synonym_name combinations. All NA Concept Ids are filtered out and the language concept id for English (4180186) is added
                        # 4. Concept Synonym Table is appended

                        #1.
                        synonyms_table <-
                                pg13::query(conn = conn,
                                            sql_statement = "SELECT DISTINCT rn_url, substance_synonym FROM chemidplus.names_and_synonyms",
                                            verbose = verbose,
                                            render_sql = render_sql) %>%
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
                                              concept_synonym_name = substance_synonym) %>%
                                dplyr::distinct() %>%
                                dplyr::filter(!is.na(concept_id)) %>%
                                dplyr::mutate(language_concept_id = 4180186)

                        #4.
                        #
                        pg13::appendTable(conn = conn,
                                          schema = destination_schema,
                                          tableName = "concept_synonym",
                                          data = concept_synonym_table)

                        if (verbose) {
                               secretary::typewrite(nrow(concept_synonym_table),
                                                 " rows added to the ChemiDPlus Concept Synonym Table.")
                        }

                } else {

                        if (verbose) {
                                secretary::typewrite("0 rows added to the ChemiDPlus Concept Table.")
                                secretary::typewrite("0 rows added to the ChemiDPlus Concept Synonym Table.")
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
                        pg13::query(conn = conn,
                                    sql_statement = "SELECT DISTINCT rn_url, substance_classification FROM chemidplus.classification;",
                                    verbose = verbose,
                                    render_sql = render_sql) %>%
                        rn_url_to_rn()

                # 2.
                classification_table_diff <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                    SqlRender::render(
                                    "
                                    WITH distinct_classes AS (
                                            SELECT DISTINCT cl.substance_classification, cl.rn_url
                                            FROM chemidplus.classification cl
                                    ),
                                    concept_classes AS (
                                            SELECT *
                                            FROM @vocab_schema.concept
                                            WHERE standard_concept = 'C'
                                    )

                                    SELECT DISTINCT d.substance_classification, d.rn_url
                                    FROM distinct_classes d
                                    LEFT JOIN concept_classes c
                                    ON c.concept_name =  d.substance_classification
                                    WHERE c.concept_id IS NULL
                                    ;
                                    ",
                                    vocab_schema = vocab_schema)) %>%
                        rn_url_to_rn()


                if (nrow(classification_table_diff)) {

                        #3
                        classification_table_diff$concept_id <-
                                sapply(1:nrow(classification_table_diff), make_identifier)

                        concept_table_diff <-
                                classification_table_diff  %>%
                                dplyr::transmute(
                                        concept_id,
                                        concept_name = substance_classification,
                                        domain_id = "Drug",
                                        vocabulary_id = "ChemiDPlus",
                                        concept_class_id = "NA",
                                        standard_concept= "C",
                                        concept_code = "NA",
                                        valid_start_date = Sys.Date(),
                                        valid_end_date = as.Date("2099-12-31"),
                                        invalid_reason = "NA") %>%
                                dplyr::filter(!is.na(concept_id)) %>%
                                dplyr::distinct()


                        pg13::appendTable(conn = conn,
                                          schema = destination_schema,
                                          tableName = "concept",
                                          data = concept_table_diff)

                        if (verbose) {

                                secretary::typewrite(nrow(concept_table_diff), " new ChemiDPlus Classification Concepts added to the ChemiDPlus Concept Table.")

                        }



                        # 4.
                        concept_ancestor_table <-
                                dplyr::left_join(classification_table_diff,
                                                 concept_table_diff,
                                                 by = "concept_id") %>%
                                dplyr::transmute(ancestor_concept_id = concept_id,
                                                 ancestor_concept_name = substance_classification,
                                                 descendant_concept_code = rn)

                        #5.
                        nonclass_concept_table <-
                                pg13::readTable(conn = conn,
                                                schema = destination_schema,
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



#' @title
#' Join ChemiDPlus Results to the OMOP Vocabulary by Concept
#'
#' @description
#' All the Names and Synonyms for a given Raw Concept are joined in lowercase to the lowercase Concept Synonyms found in the OMOP Concept Synonym Table that are valid and belong to the Drug domain.
#'
#' @param conn                  Postgres connection
#' @param chemidplusSchema      Schema that contains the Registry Number Log and Names and Synonyms` Tables
#' @param vocabularySchema      Schema that contains the OMOP Concept and Concept Synonym Tables, Default: 'public'
#' @param vocabularyIds         Vocabulary Ids in OMOP Concept Table to filter results for. Default: c("HemOnc", "RxNorm", "RxNorm Extension", "ATC")
#'
#' @return
#' Dataframe with all the Raw Concepts from the Registry Number Log with a Registry Number,  the Timestamp of when the Names and Synonyms Table was appended, Registry Number URL, the Synonym Type and Synonym scraped from the URL, and all the fields found in the OMOP Concept Table that the Synonym mapped to via any of the OMOP Concept Synonyms (not returned).
#'
#' @seealso
#'  \code{\link[pg13]{query}}
#'  \code{\link[SqlRender]{render}}
#'
#' @rdname join_chemidplus_omop_synonyms
#' @export
#' @importFrom pg13 query
#' @importFrom SqlRender render




join_chemidplus_omop_synonyms <-
        function(conn,
                 chemidplusSchema,
                 vocabularySchema = "public",
                 vocabularyIds = c('HemOnc', 'RxNorm', 'RxNorm Extension', 'ATC')) {


                #conn <- chariot::connectAthena()
                #
                vocabularyIds <- paste0("'", vocabularyIds, "'")

                pg13::query(conn = conn,
                            sql_statement =
                                    SqlRender::render(
                                            "
                                                SELECT rnl.raw_concept, nas.*, c.*
                                                FROM @chemidplusSchema.registry_number_log rnl
                                                LEFT JOIN @chemidplusSchema.names_and_synonyms nas
                                                ON rnl.rn_url = nas.rn_url
                                                LEFT JOIN @vocabularySchema.concept_synonym cs
                                                ON LOWER(cs.concept_synonym_name) = LOWER(nas.concept_synonym_name)
                                                LEFT JOIN @vocabularySchema.concept c
                                                ON c.concept_id = cs.concept_id
                                                WHERE
                                                        c.invalid_reason IS NULL AND
                                                        c.domain_id = 'Drug' AND
                                                        c.vocabulary_id IN (@vocabularyIds)
                                                        AND rnl.rn_url IS NOT NULL
                                                ;
                                                ",
                                            chemidplusSchema = chemidplusSchema,
                                            vocabularySchema = vocabularySchema,
                                            vocabularyIds = vocabularyIds))

        }
