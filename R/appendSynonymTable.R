#' Create CONCEPT_SYNONYM Table
#' @importFrom pg13 writeTable
#' @importFrom pg13 lsTables
#' @import dplyr
#' @export

appendSynonymTable <-
    function(conn,
             .input) {

                if (nrow(.input) == 0) {
                    stop("input is empty")
                }

                concept_synonym_table <- pg13::query(conn = conn,
                                             pg13::buildQuery(schema = "cancergov",
                                                              tableName = "concept_synonym"))


                if (nrow(concept_synonym_table) == 0) {
                    concept_synonym <-
                        .input %>%
                        rubix::filter_at_grepl(concept_definition,
                                               grepl_phrase = "[(]{1}other name for[:]{1}") %>%
                        dplyr::transmute(concept_id,
                                         concept_synonym_name = stringr::str_replace_all(concept_definition, "(^[(]{1}.*?[:]{1} )(.*?)([)]{1})", "\\2")) %>%
                        dplyr::mutate(concept_synonym_name = sapply(concept_synonym_name, centipede::in_title_format)) %>%
                        dplyr::distinct() %>%
                        dplyr::mutate(language_concept_id = 4180186)

                    pg13::appendTable(conn = conn,
                                     schema = "cancergov",
                                     tableName = "concept_synonym",
                                     .data = concept_synonym %>%
                                         dplyr::select(concept_id,
                                                       concept_synonym_name,
                                                       language_concept_id) %>%
                                         as.data.frame())
                }

    }



