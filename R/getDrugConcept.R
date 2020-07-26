#' Get Individual Drug Data
#' @importFrom purrr map
#' @importFrom centipede no_blank
#' @import dplyr
#' @export

getDrugConcept <-
    function(drug_urls,
             starting_row,
             ending_row) {


                .input <- loadScrapedDrugs(drug_urls,
                                           starting_row = starting_row,
                                           ending_row = ending_row)

                .output <-
                    .input %>%
                            purrr::keep(~!is.null(.)) %>%
                            purrr::map(function(x) x %>%
                                                        strsplit(split = "[ ]{0,}\r\n[ ]{0,}") %>%
                                                        unlist() %>%
                                                        centipede::no_blank() %>%
                                                        t() %>%
                                                        as.data.frame()) %>%
                            dplyr::bind_rows()

                colnames(.output) <- c("concept_name",
                                       "concept_definition2",
                                       "concept_synonym_name")


                # Is this input already in concept table?
                concept_table <- chariot::query_athena("SELECT * FROM cancergov.concept", override_cache = TRUE)

                .output2 <-
                    .output %>%
                    dplyr::left_join(concept_table,
                                     by = "concept_name") %>%
                    dplyr::distinct() %>%
                    dplyr::select(concept_id,
                                  concept_synonym_name)

                qa <- .output2 %>%
                            dplyr::filter(is.na(concept_id))

                if (nrow(qa)) {
                    flagGetDrugConcept <<- qa
                    warning(nrow(flagGetDrugConcept), " concepts do not have a concept id. See flagGetDrugConcept.")
                }

                synonym_types <- c("Abbreviation:",
                                   "Synonym:",
                                   "Code name:",
                                   "Foreign brand name:",
                                   "US brand name:",
                                   "Chemical structure:",
                                   "Acronym:")

                .output3 <- list()

                for (i in 1:length(synonym_types)) {
                    .output3[[i]] <-
                        .output2 %>%
                        tidyr::extract(col = concept_synonym_name,
                                       into = c("Label1", synonym_types[i], "Label2"),
                                       regex = paste0("(", synonym_types[i],")","(.*?)","(", paste(synonym_types, collapse = "|"), ".*$)"),
                                       remove = FALSE) %>%
                        dplyr::select(-concept_synonym_name,
                                      -Label1,
                                      -Label2)
                }

                .output4 <-
                    .output3 %>%
                    purrr::reduce(left_join) %>%
                    dplyr::distinct() %>%
                    tidyr::pivot_longer(cols = !concept_id,
                                        names_to = "Synonym",
                                        values_to = "concept_synonym_name") %>%
                    dplyr::filter(!is.na(concept_synonym_name)) %>%
                    dplyr::select(-Synonym)

                concept_synonym_table <- chariot::query_athena("SELECT * FROM cancergov.concept_synonym", override_cache = TRUE) %>%
                    dplyr::mutate(col_nchar = nchar(concept_synonym_name)) %>%
                    dplyr::mutate(in_concept_synonym_table = TRUE)

                .output5 <- .output4 %>%
                            dplyr::left_join(concept_synonym_table) %>%
                            dplyr::distinct() %>%
                            dplyr::mutate(in_concept_synonym_table = ifelse(is.na(in_concept_synonym_table), FALSE, in_concept_synonym_table)) %>%
                            dplyr::filter(in_concept_synonym_table == FALSE) %>%
                            dplyr::filter(col_nchar <= 255) %>%
                            dplyr::select(concept_id, concept_synonym_name)  %>%
                            dplyr::mutate(language_concept_id = 4180186) %>%
                            dplyr::distinct()


                return(.output5)
    }
