#' Append Synonym Table
#' @import dplyr
#' @import purrr
#' @import chariot
#' @import tidyr
#' @import pg13
#' @export

addSynonymTable <-
    function(.input,
             conn) {
        # Is this input already in concept table?
        concept_table <- pg13::query(conn = conn,
                                     pg13::buildQuery(schema = "cancergov",
                                                      tableName = "concept"))

        .input2_concept <-
            .input %>%
            dplyr::select(-any_of("concept_id")) %>%
            dplyr::left_join(concept_table,
                             by = "concept_name") %>%
            dplyr::distinct() %>%
            dplyr::select(concept_id,
                          concept_synonym_name)

        qa <- .input2_concept %>%
            dplyr::filter(is.na(concept_id))

        if (nrow(qa)) {
                flagAppendSynonymTable <<- qa
                warning(nrow(flagAppendSynonymTable), " concepts do not have a concept id. See flagAppendSynonymTable.")
        }

        synonym_types <- c("Abbreviation:",
                           "Synonym:",
                           "Code name:",
                           "Foreign brand name:",
                           "US brand name:",
                           "Chemical structure:",
                           "Acronym:")

        .output <- list()

        for (i in 1:length(synonym_types)) {
            .output[[i]] <-
                .input2_concept %>%
                tidyr::extract(col = concept_synonym_name,
                               into = c("Label1", synonym_types[i], "Label2"),
                               regex = paste0("(", synonym_types[i],")","(.*?)","(", paste(synonym_types, collapse = "|"), ".*$)"),
                               remove = FALSE) %>%
                dplyr::select(-concept_synonym_name,
                              -Label1,
                              -Label2)
        }

        .output2 <-
            .output %>%
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
        .output3 <-
            .output2 %>%
            dplyr::left_join(concept_synonym_table) %>%
            dplyr::distinct() %>%
            dplyr::mutate(in_concept_synonym_table = ifelse(is.na(in_concept_synonym_table), FALSE, in_concept_synonym_table)) %>%
            dplyr::filter(in_concept_synonym_table == FALSE) %>%
            dplyr::filter(col_nchar <= 255) %>%
            dplyr::select(concept_id, concept_synonym_name)  %>%
            dplyr::mutate(language_concept_id = 4180186) %>%
            dplyr::distinct()

        pg13::appendTable(conn = conn,
                         schema = "cancergov",
                         tableName = "concept_synonym",
                         .data = .output3 %>%
                             as.data.frame())
    }
