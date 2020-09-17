library(tidyverse)
conn <- chariot::connectAthena()
# pg13::dropTable(conn = conn,
#                 schema = "chemidplus",
#                 tableName = "registry_number_log")

# Move saved phrase_log to registry_number_log
# devtools::install_github("meerapatelmd/chemidplusData")
# library(chemidplusData)
#
# phrase_log <- chemidplusData::PHRASE_LOG

phrase_log <-
        pg13::readTable(conn = conn,
                        schema = "chemidplus",
                        tableName = "phrase_log")

registry_number_log <-
        phrase_log %>%
        dplyr::rename(rnl_datetime = phrase_datetime,
                      raw_concept = input,
                      processed_concept = phrase) %>%
        dplyr::mutate_at(vars(compound_match,
                              rn,
                              rn_url),
                         ~na_if(., "NA"))


pg13::writeTable(conn = conn,
                 schema = "chemidplus",
                 tableName = "registry_number_log",
                 registry_number_log)


# pg13::dropTable(conn = conn,
#                 schema = "chemidplus",
#                 tableName = "rn_url_validity")

#Move synonyms to NAMES_AND_SYNONYMS

synonyms <-
        pg13::readTable(conn = conn,
                        schema = "chemidplus",
                        tableName = "synonyms")



names_and_synonyms <-
        synonyms %>%
        dplyr::rename(nas_datetime = scrape_datetime) %>%
        rubix::normalize_at_to_na(concept_synonym_type) %>%
        dplyr::mutate(nas_datetime = as.POSIXct(nas_datetime))


pg13::dropTable(conn = conn,
                 schema = "chemidplus",
                 tableName = "names_and_synonyms")

pg13::writeTable(conn = conn,
                 schema = "chemidplus",
                 tableName = "names_and_synonyms",
                 names_and_synonyms)


classification <-
        pg13::readTable(conn = conn,
                        schema = "chemidplus",
                        tableName = "classification") %>%
        dplyr::rename(c_datetime = scrape_datetime) %>%
        rubix::mutate_all_na_str_to_na() %>%
        dplyr::mutate(c_datetime = as.POSIXct(c_datetime))



pg13::renameTable(conn = conn,
                schema = "chemidplus",
                tableName = "classification",
                newTableName = "saved")

pg13::writeTable(conn = conn,
                 schema = "chemidplus",
                 tableName = "classification",
                 classification)

pg13::dropTable(conn = conn,
                schema = "chemidplus",
                tableName = "saved")



registry_number_log <-
        pg13::readTable(conn = conn,
                        schema = "chemidplus",
                        tableName = "registry_number_log")

pg13::renameTable(conn = conn,
                  schema = "chemidplus",
                  tableName = "registry_number_log",
                  newTableName = "saved")


new_registry_number_log <-
        registry_number_log %>%
        mutate(compound_match = na_if(compound_match, "NA")) %>%
        mutate(rn = na_if(rn, "NA")) %>%
        mutate(rn_url = na_if(rn_url, "NA"))

pg13::writeTable(conn = conn,
                 schema = "chemidplus",
                 tableName = "registry_number_log",
                 new_registry_number_log)
