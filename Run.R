conn <- chariot::connect_athena()
input <- getDrugDictionaryConcept(max_page = 39)
createCancerGovSchema(conn = conn)
createDefinitionTable(conn = conn,
                      .input = input)
createMetadataTable(conn = conn,
                    .input = input)
createSynonymTable(conn = conn,
                   .input = input)
createConceptTable(conn = conn,
                   .input = input)


scrapeDrugPages(max_page = 39)


input2 <- getDrugConcept(conn = conn,
                         max_page = 5)


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
input2 %>%
    tidyr::extract(col = V3,
                  into = c(synonym_types[i], "Value", "Label2"),
                   regex = paste0("(", synonym_types[i],")","(.*?)","([", paste(synonym_types, collapse = "|"), "].*$)"))
}





"Abbreviation:|Synonym:|Code name:|Foreign brand name:|US brand name:|Chemical structure:|Acronym:"



data.frame(type =
            input2$V3 %>%
                strsplit(split = ":") %>%
                unlist()
) %>%
    rubix::summarize_grouped_n(type,
                               desc = TRUE)

# input2 <- loadScrapedDrugs(max_page = 39)
# output <-
#     input2 %>%
#     purrr::map(function(x) data.frame(t(x))) %>%
#     dplyr::bind_rows()
#
#
# data.frame(t(1:3))
#
#
# output <- getDrugPageLinks(max_page = 39)
#
# scrapeDrugPage(output)
