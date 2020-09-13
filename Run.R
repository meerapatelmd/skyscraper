conn <- chariot::connectAthena()
pg13::dropSchema(conn = conn,
                 schema = "cancergov",
                 cascade = TRUE)
skyscraper::createCancerGovSchema(conn = conn)
## OMOP Vocabulary Tables + CONCEPT_DEFINITION and CONCEPT_METADATA tables
skyscraper::ddlCancerGovSchema(conn = conn)

drugCount <- skyscraper::nciDrugCount()
stopifnot(is.integer(drugCount))

concept_log <-
        pg13::readTable(conn = conn,
                        schema = "cancergov",
                        tableName = "concept_log")
stopifnot(!(drugCount %in% concept_log$concept_count))

pg13::appendTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept_log",
                  data.frame(concept_timestamp = as.character(Sys.time()),
                            concept_count = drugCount))

drugLinks <- nciDrugDetailLinks()
stopifnot(nrow(drugLinks) == drugCount)

scrapeDrugSynonymsRandom(df=drugLinks,
                         progress_bar = FALSE)
loadCachedDrugSynonyms(df=drugLinks)

stopifnot(length(loadCachedDrugSynonyms_results)==drugCount)
failed_to_scrape <-
    loadCachedDrugSynonyms_results %>%
    purrr::keep(function(x) any(is.na(x$X1)))

not_scraped <-
    loadCachedDrugSynonyms_results %>%
    purrr::keep(function(x) is.null(x))

drugData <-
    loadCachedDrugSynonyms_results %>%
    dplyr::bind_rows(.id = "Label")

drugData2a <-
    drugData %>%
    dplyr::select(Label) %>%
    dplyr::distinct() %>%
    dplyr::transmute(concept_id = NA,
                     concept_name = Label,
                     domain_id = "Drug",
                     vocabulary_id = "NCI Drug Dictionary",
                     concept_class_id = "Label",
                     standard_concept = NA,
                     concept_code = NA,
                     valid_start_date = Sys.Date(),
                     valid_end_date = as.Date("2099-12-31"),
                     invalid_reason = NA)

drugData2a$concept_id <-
    rubix::make_identifier()+1:nrow(drugData2a)

concept_table <- drugData2a

drugData2a2 <-
    drugData %>%
    dplyr::left_join(concept_table,
                     by = c("Label" = "concept_name")) %>%
    dplyr::distinct()

any(is.na(drugData2a2$concept_id))

concept_synonym_table <-
    drugData2a2 %>%
    dplyr::select(concept_id,
                  concept_name = Label,
                  concept_name_2 = X2) %>%
    #Remove concept_name_2 that are NA, meaning that no Synonyms were found
    dplyr::filter(!is.na(concept_name_2)) %>%
    dplyr::distinct() %>%
    tidyr::pivot_longer(cols = !concept_id,
                        values_to = "concept_synonym_name",
                        values_drop_na = TRUE) %>%
    dplyr::transmute(concept_id,
                     concept_synonym_name,
                     language_concept_id = 4180186) %>%
    dplyr::distinct()


concept_relationship_table <-
    drugData2a2 %>%
    dplyr::select(concept_id_1 = concept_id,
                  concept_name_1 = Label,
                  relationship = X1,
                  concept_name_2 = X2) %>%
    dplyr::filter(!is.na(concept_name_2)) %>%
    dplyr::left_join(concept_synonym_table,
                     by = c("concept_name_2" = "concept_synonym_name")) %>%
    dplyr::rename(concept_id_2 = concept_id) %>%
    dplyr::select(-language_concept_id) %>%
    # Modify Relationship
    dplyr::mutate(relationship_id = paste0("Has ", tolower(stringr::str_remove_all(relationship, pattern = "[[:punct:]]{1,}$")))) %>%
    dplyr::select(-relationship) %>%
    dplyr::transmute(concept_id_1,
                     concept_id_2,
                     relationship_id,
                     valid_start_date = Sys.Date(),
                     valid_end_date = NA,
                     invalid_reason = NA) %>%
    dplyr::distinct()

all(!is.na(concept_relationship_table$concept_id_1))
all(!is.na(concept_relationship_table$concept_id_2))

concept_relationship_table_inverse <-
    concept_relationship_table %>%
    dplyr::transmute(concept_id_1 = concept_id_2,
                     concept_id_2 = concept_id_1,
                     relationship_id = paste0(stringr::str_to_title(stringr::str_remove_all(relationship_id, "Has ")), " of"),
                     valid_start_date,
                     valid_end_date,
                     invalid_reason) %>%
    dplyr::distinct()


concept_relationship_table <-
    dplyr::bind_rows(concept_relationship_table,
                     concept_relationship_table_inverse) %>%
    dplyr::distinct()


pg13::dropTable(conn = conn,
                schema = "cancergov",
                tableName = "concept")

pg13::writeTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept",
                  concept_table)

pg13::dropTable(conn = conn,
                schema = "cancergov",
                tableName = "concept_synonym")

pg13::writeTable(conn = conn,
                 schema = "cancergov",
                 tableName = "concept_synonym",
                 concept_synonym_table)

pg13::dropTable(conn = conn,
                schema = "cancergov",
                tableName = "concept_relationship")

pg13::writeTable(conn = conn,
                 schema = "cancergov",
                 tableName = "concept_relationship",
                 concept_relationship_table)


nciDD <- skyscraper::scrapeNCIDrugDict(max_page = 39)
stopifnot(nrow(nciDD) == drugCount)
pg13::dropTable(conn = conn,
                schema = "cancergov",
                tableName = "drug_dictionary")
pg13::writeTable(conn = conn,
                 schema = "cancergov",
                 tableName = "drug_dictionary",
                 nciDD)

# concept_definition <-
#         pg13::readTable(conn = conn,
#                         schema = "cancergov",
#                         tableName = "concept_definition")

concept_synonym <-
    pg13::readTable(conn = conn,
                    schema = "cancergov",
                    tableName = "concept_synonym")


definition_to_id <-
    pg13::query(conn = conn,
                sql_statement = "SELECT cancergov.drug_dictionary.*, cancergov.concept_synonym.concept_id
                                FROM cancergov.drug_dictionary
                                LEFT JOIN cancergov.concept_synonym
                                    ON LOWER(cancergov.concept_synonym.concept_synonym_name) = LOWER(cancergov.drug_dictionary.drug);") %>%
    tibble::as_tibble()


new_concepts <-
    definition_to_id %>%
    dplyr::filter(is.na(concept_id)) %>%
    dplyr::group_by(drug) %>%
    dplyr::mutate(instance = 1:n()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(instance == 1) %>%
    dplyr::select(-instance) %>%
    dplyr::distinct()

# Get max ID from today
max_concept_id <-
chariot::queryAthena(pg13::buildQuery(fields = "concept_id",
                                      distinct = TRUE,
                                      schema = "cancergov",
                                      tableName = "concept",
                                      whereInField = "valid_start_date",
                                      whereInVector = as.character(Sys.Date()),
                                      caseInsensitive = FALSE)) %>%
    max()
new_concept_ids <- max_concept_id+1:nrow(new_concepts)
new_concepts$concept_id <- new_concept_ids


append_to_concept_table <-
    new_concepts %>%
dplyr::transmute(concept_id,
                 concept_name = drug,
                 domain_id = "Drug",
                 vocabulary_id = "NCI Drug Dictionary",
                 concept_class_id = "Label",
                 standard_concept = NA,
                 concept_code = NA,
                 valid_start_date = Sys.Date(),
                 valid_end_date = as.Date("2099-12-31"),
                 invalid_reason = NA) %>%
    dplyr::distinct()

pg13::appendTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept",
                  append_to_concept_table)

append_to_concept_synonym_table <-
    append_to_concept_table %>%
    dplyr::transmute(concept_id,
                  concept_synonym_name = concept_name,
                  language_concept_id = 4180186) %>%
    dplyr::distinct()


pg13::appendTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept_synonym",
                  append_to_concept_synonym_table)


definition_to_id <-
    pg13::query(conn = conn,
                sql_statement = "SELECT cancergov.drug_dictionary.*, cancergov.concept_synonym.concept_id
                                FROM cancergov.drug_dictionary
                                LEFT JOIN cancergov.concept_synonym
                                    ON LOWER(cancergov.concept_synonym.concept_synonym_name) = LOWER(cancergov.drug_dictionary.drug);") %>%
    tibble::as_tibble()


all(!is.na(definition_to_id))

concept_definition_table <-
    definition_to_id %>%
    dplyr::select(concept_id,
                  concept_name = drug,
                  concept_definition = definition) %>%
    dplyr::distinct()

pg13::appendTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept_definition",
                  concept_definition_table)

pg13::dropTable(conn = conn,
                schema = "cancergov",
                tableName = "drug_dictionary")





