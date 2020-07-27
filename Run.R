# conn <- chariot::connect_athena()
#
# createCancerGovSchema(conn = conn)
# ddlCancerGovSchema(conn = conn)
# input <- getDrugDictionaryConcept(max_page = 39)
# createDefinitionTable(conn = conn,
#                       .input = input)
# createMetadataTable(conn = conn,
#                     .input = input)
# appendSynonymTable(conn = conn,
#                    .input = input)
# appendConceptTable(conn = conn,
#                    .input = input)
#
#
#  drug_links <- getDrugPageLinks(max_page = 39)
#  scrapedHtml <- loadScrapedDrugs(drug_links,
#                                  1,
#                                  7795)
#  names(scrapedHtml) <- drug_links$DRUG
#
#  scrapedHtml2 <-
#  scrapedHtml %>%
#      purrr::keep(~!is.null(.)) %>%
#      purrr::map(function(x) x %>%
#                     strsplit(split = "[ ]{0,}\r\n[ ]{0,}") %>%
#                     unlist() %>%
#                     centipede::no_blank() %>%
#                     t() %>%
#                     as.data.frame()) %>%
#      dplyr::bind_rows(.id = "concept_name") %>%
#      dplyr::select(concept_name,
#                    concept_synonym_name = V3)
#
#  concept_table <- chariot::query_athena("SELECT * FROM cancergov.concept", override_cache = TRUE)
#
# scrapedHtml3 <-
#      scrapedHtml2 %>%
#      dplyr::left_join(concept_table,
#                       by = "concept_name") %>%
#      dplyr::distinct() %>%
#      dplyr::select(concept_id,
#                    concept_synonym_name) %>%
#     dplyr::filter(!is.na(concept_id))
#
# all(scrapedHtml3$concept_id %in% concept_table$concept_id)
#
#
# synonym_types <- c("Abbreviation:",
#                    "Synonym:",
#                    "Code name:",
#                    "Foreign brand name:",
#                    "US brand name:",
#                    "Chemical structure:",
#                    "Acronym:")
#
# output <- list()
#
# for (i in 1:length(synonym_types)) {
#     output[[i]] <-
#         scrapedHtml3 %>%
#         dplyr::filter(!is.na(concept_synonym_name)) %>%
#         tidyr::extract(col = concept_synonym_name,
#                        into = c("Label1", synonym_types[i], "Label2"),
#                        regex = paste0("(", synonym_types[i],")","(.*?)","(", paste(synonym_types, collapse = "|"), ".*$)"),
#                        remove = FALSE) %>%
#         dplyr::select(-concept_synonym_name,
#                       -Label1,
#                       -Label2) %>%
#         dplyr::filter_at(vars(!concept_id),
#                          any_vars(!is.na(.)))
# }
#
# output2 <-
#     output %>%
#     purrr::map(function(x) x %>%
#                             dplyr::rename_at(vars(2), function(x) paste("concept_synonym_name"))) %>%
#     dplyr::bind_rows() %>%
#     dplyr::distinct() %>%
#     dplyr::filter(!is.na(concept_synonym_name))
#
# concept_synonym_table <- chariot::query_athena("SELECT * FROM cancergov.concept_synonym", override_cache = TRUE) %>%
#     dplyr::mutate(in_concept_synonym_table = TRUE)
#
# output3 <-
#     output2 %>%
#     dplyr::left_join(concept_synonym_table) %>%
#     dplyr::distinct() %>%
#     dplyr::mutate(in_concept_synonym_table = ifelse(is.na(in_concept_synonym_table), FALSE, in_concept_synonym_table)) %>%
#     dplyr::filter(in_concept_synonym_table == FALSE) %>%
#     dplyr::select(concept_id, concept_synonym_name)  %>%
#     dplyr::mutate(language_concept_id = 4180186) %>%
#     dplyr::distinct()
#
# pg13::appendTable(conn = conn,
#                   schema = "cancergov",
#                   tableName = "concept_synonym",
#                   .data = output3 %>%
#                       as.data.frame())
