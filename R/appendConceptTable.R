#' Create CONCEPT Table
#' @importFrom pg13 writeTable
#' @importFrom pg13 lsTables
#' @import dplyr
#' @export

appendConceptTable <-
    function(conn,
             .input) {

                if (nrow(.input) == 0) {
                    stop("input is empty")
                }

                conceptTable <- pg13::query(conn = conn,
                                            pg13::buildQuery(schema = "cancergov",
                                                             tableName = "concept"))

                if (nrow(conceptTable) == 0) {
                    concept <-
                        input %>%
                        rubix::filter_at_grepl(concept_definition,
                                               grepl_phrase = "[(]{1}other name for[:]{1}",
                                               evaluates_to = FALSE) %>%
                        dplyr::transmute(concept_id,
                                         concept_name,
                                         domain_id,
                                         vocabulary_id,
                                         concept_class_id,
                                         standard_concept = NA,
                                         concept_code = "0",
                                         valid_start_date = Sys.Date(),
                                         valid_end_date = as.Date("2099-12-31"),
                                         invalid_reason = NA)

                    pg13::appendTable(conn = conn,
                                     schema = "cancergov",
                                     tableName = "concept",
                                     .data = concept %>%
                                         as.data.frame())
                }

    }



