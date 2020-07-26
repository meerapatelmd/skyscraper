#' Create CONCEPT_SYNONYM Table
#' @importFrom pg13 writeTable
#' @importFrom pg13 lsTables
#' @import dplyr
#' @export

createSynonymTable <-
    function(conn,
             .input) {

                if (nrow(.input) == 0) {
                    stop("input is empty")
                }

                cgTables <- pg13::lsTables(conn = conn,
                                           schema = "cancergov")


                if (!("concept_synonym" %in% tolower(cgTables))) {
                    concept_synonym <-
                        .input %>%
                        rubix::filter_at_grepl(concept_definition,
                                               grepl_phrase = "[(]{1}other name for[:]{1}") %>%
                        dplyr::transmute(concept_id,
                                         concept_synonym_name = stringr::str_replace_all(concept_definition, "(^[(]{1}.*?[:]{1} )(.*?)([)]{1})", "\\2")) %>%
                        dplyr::mutate(concept_synonym_name = sapply(concept_synonym_name, centipede::in_title_format)) %>%
                        dplyr::distinct() %>%
                        dplyr::mutate(language_concept_id = 4180186)

                    pg13::writeTable(conn = conn,
                                     schema = "cancergov",
                                     tableName = "concept_synonym",
                                     .data = concept_synonym %>%
                                         dplyr::select(concept_id,
                                                       concept_synonym_name,
                                                       language_concept_id) %>%
                                         as.data.frame())
                }

    }



