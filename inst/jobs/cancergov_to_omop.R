library(tidyverse)
library(skyscraper)
library(chariot)

conn <- chariot::connectAthena()
drug_link_table <-
        pg13::readTable(conn = conn,
                        schema = "cancergov",
                        tableName = "drug_link")
drug_link_synonym_table <-
        pg13::readTable(conn = conn,
                        schema = "cancergov",
                        tableName = "drug_link_synonym")
chariot::dcAthena(conn = conn,
                  remove = TRUE)


concept_table <-
        drug_link_table %>%
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


concept_table$concept_id <-
        rubix::make_identifier()+1:nrow(concept_table)


concept_synonym_table <-
        drug_link_synonym_table %>%
                dplyr::left_join(concept_table,
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
pg13::writeTable(conn = conn,
                 schema = "cancergov",
                 tableName = "concept",
                 concept_table)

pg13::writeTable(conn = conn,
                 schema = "cancergov",
                 tableName = "concept_synonym",
                 concept_synonym_table)


concept_definition_table <-
pg13::query(conn = conn,
            sql_statement =
            "SELECT DISTINCT concept_id, concept_name, definition AS concept_definition
            FROM cancergov.concept
            LEFT JOIN cancergov.drug_dictionary
                ON cancergov.drug_dictionary.drug = cancergov.concept.concept_name
            ;")

pg13::writeTable(conn = conn,
                 schema = "cancergov",
                 tableName = "concept_definition",
                 concept_definition_table)


chariot::dcAthena(conn = conn,
                  remove = TRUE)

