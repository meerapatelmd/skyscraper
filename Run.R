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
                     valid_end_date = NA,
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
                     language_concept_id = )

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

new_concepts <-
    pg13::query(conn = conn,
                sql_statement = "SELECT cancergov.drug_dictionary.*, cancergov.concept_synonym.concept_id
                                FROM cancergov.drug_dictionary
                                LEFT JOIN cancergov.concept_synonym
                                    ON cancergov.concept_synonym.concept_synonym_name = cancergov.drug_dictionary.drug;") %>%
    tibble::as_tibble() %>%
    dplyr::filter(is.na(concept_id)) %>%
    dplyr::group_by(drug) %>%
    dplyr::mutate(instance = 1:n()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(instance == 1) %>%
    dplyr::select(-instance) %>%
    dplyr::distinct()

# Add UUID
new_concept_ids <- rubix::make_identifier()+1:nrow(new_concepts)
new_concepts$concept_id <- new_concept_ids


# Deprecate New Concepts that are actually synonyms
new_concept_synonyms <-
    new_concepts %>%
    rubix::filter_at_grepl(definition,
                           grepl_phrase = "[(]{1}Other name for[:]{1} .*[)]{1}$")


new_concepts <-
    new_concepts %>%
    rubix::filter_at_grepl(definition,
                           grepl_phrase = "[(]{1}Other name for[:]{1} .*[)]{1}$",
                           evaluates_to = FALSE)

# Pair New Concept Synonyms with the appropriate New Concept to make sure maps back to the correct concept_id
new_concept_synonyms <-
    new_concept_synonyms %>%
        dplyr::mutate(lower_concept_name = tolower(stringr::str_remove_all(definition, "[(]{1}Other name for[:]{1} |[)]{1}$"))) %>%
        dplyr::left_join(new_concepts %>%
                             dplyr::mutate(lower_concept_name = tolower(drug)),
                         by = "lower_concept_name",
                         suffix = c(".cs", ".c")) %>%
        dplyr::select(concept_id = concept_id.c,
                      concept_synonym_name = drug.cs,
                      everything()) %>%
        dplyr::distinct() %>%
        dplyr::group_by(concept_synonym_name) %>%
        dplyr::mutate(instance = 1:n()) %>%
        dplyr::ungroup() %>%
        dplyr::filter(instance == 1) %>%
        dplyr::select(-instance) %>%
        dplyr::distinct()

qa <-
new_concept_synonyms %>%
    dplyr::filter(is.na(concept_id))

stopifnot(nrow(qa) == 0)

pg13::appendTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept_synonym",
                  new_concept_synonyms %>%
                      dplyr::transmute(concept_id,
                                    concept_synonym_name,
                                    language_concept_id = 4180186) %>%
                      dplyr::distinct())

pg13::appendTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept_synonym",
                  new_concepts %>%
                      dplyr::transmute(concept_id,
                                       concept_synonym_name = drug,
                                       language_concept_id = 4180186) %>%
                      dplyr::distinct())

pg13::appendTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept_definition",
                  new_concepts %>%
                      dplyr::select(concept_id,
                                    concept_name = drug,
                                    concept_definition = definition) %>%
                      dplyr::distinct())

pg13::dropTable(conn = conn,
                schema = "cancergov",
                tableName = "drug_dictionary")


##########################
drugLinks <- nciDrugDetailLinks()
stopifnot(nrow(drugLinks) == drugCount)

pg13::dropTable(conn = conn,
                 schema = "cancergov",
                 tableName = "drug_link")


pg13::writeTable(conn = conn,
                 schema = "cancergov",
                 tableName = "drug_link",
                 drugLinks)

# new_concepts <-
# pg13::query(conn = conn,
#             sql_statement =  "SELECT dl.*,cs.concept_id
#                                 FROM cancergov.drug_link dl
#                                 LEFT JOIN cancergov.concept_synonym cs
#                                 ON dl.drug = cs.concept_synonym_name")
#
#
# qa <- new_concepts %>%
#         dplyr::filter(is.na(concept_id))
# stopifnot(nrow(qa) == 0)

# pg13::dropTable(conn = conn,
#                 schema = "cancergov",
#                 tableName = "drug_link")


output <- scrapeDrugSynonyms(df=new_concepts)


output <- loadCachedDrugSynonyms(df=new_concepts)

drugs_with_no_synonyms <-
output %>%
    dplyr::filter_at(vars(!drug),
                     all_vars(is.na(.))) %>%
    dplyr::select(drug) %>%
    dplyr::distinct() %>%
    dplyr::left_join(output)

drugs_with_synonyms <-
    output %>%
    dplyr::filter_at(vars(!drug),
                     any_vars(!is.na(.))) %>%
    dplyr::distinct() %>%
    dplyr::rename(relationship = X1,
                  concept_name_2 = X2) %>%
    dplyr::mutate_at(vars(relationship), stringr::str_remove_all, "[[:punct:]]{1,}$")

pg13::dropTable(conn = conn,
                schema = "cancergov",
                tableName = "drugs_with_synonyms")


pg13::writeTable(conn = conn,
                 schema = "cancergov",
                 tableName = "drugs_with_synonyms",
                 drugs_with_synonyms)

new_concepts <-
    pg13::query(conn = conn,
                sql_statement =  "SELECT dl.*,cs.concept_id as concept_id_1,cs2.concept_id as concept_id_2
                                FROM cancergov.drugs_with_synonyms dl
                                LEFT JOIN cancergov.concept_synonym cs
                                ON dl.drug = cs.concept_synonym_name
                                LEFT JOIN cancergov.concept_synonym cs2
                                ON LOWER(dl.concept_name_2) = LOWER(cs2.concept_synonym_name)")

all(!is.na(new_concepts$concept_id_1))

new_concepts <-
    new_concepts %>%
    dplyr::select(-drug) %>%
    dplyr::filter(relationship != "Chemical structure")  %>%
    dplyr::distinct()


na_concept_2_ct <- length(new_concepts$concept_id_2)
new_concepts$concept_id_2_2 <- rubix::make_identifier()+1:na_concept_2_ct
new_concepts <-
    new_concepts %>%
    dplyr::mutate(concept_id_2 = coalesce(concept_id_2,
                                          concept_id_2_2)) %>%
    dplyr::select(-concept_id_2_2)


all(!is.na(new_concepts$concept_id_2))

new_concepts2 <-
    split(new_concepts,
          new_concepts$relationship)

concept_table_a <-
    new_concepts2 %>%
    dplyr::bind_rows(.id = "concept_class_id") %>%
    dplyr::transmute(concept_id = concept_id_2,
                  concept_name = concept_name_2,
                  domain_id = "Drug",
                  vocabulary_id = "NCI Drug Dictionary",
                  concept_class_id,
                  standard_concept = NA,
                  concept_code = NA,
                  valid_start_date = as.character(Sys.Date()),
                  valid_end_date = NA,
                  instance = 2
                  ) %>%
    dplyr::distinct()



concept_table_b <-
    pg13::query(conn = conn,
                sql_statement =  "SELECT DISTINCT dl.*,cs.concept_id
                                FROM cancergov.drugs_with_synonyms dl
                                LEFT JOIN cancergov.concept_synonym cs
                                ON dl.drug = cs.concept_synonym_name
                                LEFT JOIN cancergov.concept_synonym cs2
                                ON LOWER(dl.concept_name_2) = LOWER(cs2.concept_synonym_name)") %>%
    dplyr::transmute(concept_id,
                     concept_name = drug,
                     domain_id = "Drug",
                     vocabulary_id = "NCI Drug Dictionary",
                     concept_class_id = "Main Label",
                     standard_concept = NA,
                     concept_code = NA,
                     valid_start_date = as.character(Sys.Date()),
                     valid_end_date = NA,
                     instance = 1
    )  %>%
    dplyr::distinct()

concept_table <-
    dplyr::bind_rows(concept_table_b,
                     concept_table_a) %>%
    dplyr::distinct() %>%
    dplyr::mutate(concept_class_id = factor(concept_class_id,
                                            levels = c("Main Label",
                                                       "Synonym",
                                                       "Abbreviation",
                                                       "Acronym",
                                                       "Code name",
                                                       "Foreign brand name",
                                                       "US brand name"))) %>%
    dplyr::group_by(concept_id) %>%
    dplyr::arrange(concept_class_id, .by_group = TRUE) %>%
    dplyr::filter(instance == min(instance)) %>%
    dplyr::filter()
    dplyr::ungroup() %>%

test_f <- factor(c("Main", "Acronym", "Synonym"))
test_f2 <-
    factor(test_f,
           levels = c("Main", "Acronym", "Synonym"))


 scrapedHtml <- loadScrapedDrugs(drug_links,
                                 1,
                                 7795)
 names(scrapedHtml) <- drug_links$DRUG

 scrapedHtml2 <-
 scrapedHtml %>%
     purrr::keep(~!is.null(.)) %>%
     purrr::map(function(x) x %>%
                    strsplit(split = "[ ]{0,}\r\n[ ]{0,}") %>%
                    unlist() %>%
                    centipede::no_blank() %>%
                    t() %>%
                    as.data.frame()) %>%
     dplyr::bind_rows(.id = "concept_name") %>%
     dplyr::select(concept_name,
                   concept_synonym_name = V3)

 concept_table <- chariot::query_athena("SELECT * FROM cancergov.concept", override_cache = TRUE)

scrapedHtml3 <-
     scrapedHtml2 %>%
     dplyr::left_join(concept_table,
                      by = "concept_name") %>%
     dplyr::distinct() %>%
     dplyr::select(concept_id,
                   concept_synonym_name) %>%
    dplyr::filter(!is.na(concept_id))

all(scrapedHtml3$concept_id %in% concept_table$concept_id)


synonym_types <- c("Abbreviation:",
                   "Synonym:",
                   "Code name:",
                   "Foreign brand name:",
                   "US brand name:",
                   "Chemical structure:",
                   "Acronym:")

output <- list()

for (i in 1:length(synonym_types)) {
    output[[i]] <-
        scrapedHtml3 %>%
        dplyr::filter(!is.na(concept_synonym_name)) %>%
        tidyr::extract(col = concept_synonym_name,
                       into = c("Label1", synonym_types[i], "Label2"),
                       regex = paste0("(", synonym_types[i],")","(.*?)","(", paste(synonym_types, collapse = "|"), ".*$)"),
                       remove = FALSE) %>%
        dplyr::select(-concept_synonym_name,
                      -Label1,
                      -Label2) %>%
        dplyr::filter_at(vars(!concept_id),
                         any_vars(!is.na(.)))
}

output2 <-
    output %>%
    purrr::map(function(x) x %>%
                            dplyr::rename_at(vars(2), function(x) paste("concept_synonym_name"))) %>%
    dplyr::bind_rows() %>%
    dplyr::distinct() %>%
    dplyr::filter(!is.na(concept_synonym_name))

concept_synonym_table <- chariot::query_athena("SELECT * FROM cancergov.concept_synonym", override_cache = TRUE) %>%
    dplyr::mutate(in_concept_synonym_table = TRUE)

output3 <-
    output2 %>%
    dplyr::left_join(concept_synonym_table) %>%
    dplyr::distinct() %>%
    dplyr::mutate(in_concept_synonym_table = ifelse(is.na(in_concept_synonym_table), FALSE, in_concept_synonym_table)) %>%
    dplyr::filter(in_concept_synonym_table == FALSE) %>%
    dplyr::select(concept_id, concept_synonym_name)  %>%
    dplyr::mutate(language_concept_id = 4180186) %>%
    dplyr::distinct()

pg13::appendTable(conn = conn,
                  schema = "cancergov",
                  tableName = "concept_synonym",
                  .data = output3 %>%
                      as.data.frame())




# If the concept never existed in the cancergov.concept_definition table, add it

input <- getDrugDictionaryConcept(max_page = 39)
createDefinitionTable(conn = conn,
                      .input = input)
createMetadataTable(conn = conn,
                    .input = input)
appendSynonymTable(conn = conn,
                   .input = input)
appendConceptTable(conn = conn,
                   .input = input)




