library(tidyverse)
library(skyscraper)
library(chariot)
library(rubix)

new_concepts <-
chariot::queryAthena("SELECT DISTINCT drug, drug_link
                     FROM cancergov.drug_link dl
                     LEFT JOIN cancergov.concept cgc
                     ON cgc.concept_name = dl.drug
                     WHERE cgc.concept_id IS NULL",
                     override_cache = TRUE)

concept_table <-
        new_concepts %>%
        dplyr::transmute(concept_id = NA,
                         concept_name = drug,
                         domain_id = "Drug",
                         vocabulary_id = "NCI Drug Dictionary",
                         concept_class_id = NA,
                         standard_concept = NA,
                         concept_code = NA,
                         valid_start_date = Sys.Date(),
                         valid_end_date = as.Date("2099-12-31"),
                         invalid_reason = NA,
                         drug_link) %>%
        dplyr::distinct()


if (nrow(concept_table)) {

        concept_table$concept_id <-
                rubix::make_identifier()+1:nrow(concept_table)


        conn <- chariot::connectAthena()
        drug_link_synonym_table <-
                pg13::readTable(conn = conn,
                                schema = "cancergov",
                                tableName = "drug_link_synonym")
        chariot::dcAthena(conn = conn,
                          remove = TRUE)


        concept_synonym_table <-
                drug_link_synonym_table %>%
                        dplyr::inner_join(concept_table,
                                         by = "drug_link") %>%
                dplyr::transmute(concept_id,
                                concept_synonym_name = x2) %>%
                dplyr::bind_rows(concept_table %>%
                                         dplyr::select(concept_id,
                                                       concept_synonym_name = concept_name)) %>%
                dplyr::mutate(language_concept_id = 4180186) %>%
                dplyr::distinct() %>%
                dplyr::filter(!is.na(concept_synonym_name))


        concept_table <-
                concept_table %>%
                dplyr::select(-drug_link) %>%
                dplyr::distinct()


        conn <- chariot::connectAthena()
        pg13::appendTable(conn = conn,
                         schema = "cancergov",
                         tableName = "concept",
                         concept_table)
        chariot::dcAthena(conn = conn,
                          remove = TRUE)

        conn <- chariot::connectAthena()
        pg13::appendTable(conn = conn,
                         schema = "cancergov",
                         tableName = "concept_synonym",
                         concept_synonym_table)
        chariot::dcAthena(conn = conn,
                          remove = TRUE)

        conn <- chariot::connectAthena()
        concept_definition_table <-
        pg13::query(conn = conn,
                    sql_statement =
                    "
                        WITH new_concepts AS (
                                    SELECT DISTINCT concept_id,concept_name
                                    FROM cancergov.concept c
                                    WHERE c.concept_id NOT IN (
                                                SELECT DISTINCT concept_id
                                                FROM cancergov.concept_definition
                                    )
                        )

                        SELECT concept_id,
                                concept_name,
                                definition AS concept_definition
                        FROM new_concepts nc
                        LEFT JOIN cancergov.drug_dictionary dd
                        ON nc.concept_name = dd.drug
                    ;")

        pg13::appendTable(conn = conn,
                         schema = "cancergov",
                         tableName = "concept_definition",
                         concept_definition_table)
        chariot::dcAthena(conn = conn,
                          remove = TRUE)


        skyscraper::export_schema_to_data_repo("~/GitHub/cancergovData/",
                                               schema = "cancergov")

}
