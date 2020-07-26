# conn <- chariot::connect_athena()
# input <- getDrugDictionaryConcept(max_page = 39)
# createCancerGovSchema(conn = conn)
# createDefinitionTable(conn = conn,
#                       .input = input)
# createMetadataTable(conn = conn,
#                     .input = input)
# createSynonymTable(conn = conn,
#                    .input = input)
# createConceptTable(conn = conn,
#                    .input = input)
#
#
# drug_links <- getDrugPageLinks(max_page = 39)
# output <- getDrugConcept(drug_links,
#                          1,
#                          4095)
# if (nrow(output)) {
#         appendSynonymTable(output,
#                            conn = conn)
# }
