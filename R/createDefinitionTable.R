#' Create CONCEPT_DEFINITION Table
#' @description The CONCEPT_DEFINITION Table is a custom addition to the OMOP Vocabulary Infrastructure and stores the human readable Data Dictionary definitions found in Cancer.gov's site.
#' @importFrom pg13 writeTable
#' @importFrom pag13 lsTables
#' @import dplyr
#' @export

createDefinitionTable <-
    function(conn = conn,
             .input) {

                if (nrow(.input) == 0) {
                    stop("input is empty")
                }

                cgTables <- pg13::lsTables(conn = conn,
                                           schema = "cancergov")


                if (!("cohort_definition" %in% tolower(cgTables))) {
                    pg13::writeTable(conn = conn,
                                     schema = "cancergov",
                                     tableName = "concept_definition",
                                     .data = .input %>%
                                         dplyr::select(concept_id,
                                                       concept_name,
                                                       concept_definition) %>%
                                         as.data.frame())
                }

    }



