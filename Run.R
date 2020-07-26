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

# fns <- list.files("R", full.names = TRUE)
# for (i in 1:length(fns)) {
#     fns_lines <- read_lines(file = fns[i])
#     if (any(grepl("pag13", fns_lines))) {
#         print(fns[i])
#         secretary::press_enter()
#     }
#
# }
