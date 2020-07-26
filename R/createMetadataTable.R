#' Create CONCEPT_METADATA Table
#' @description The CONCEPT_METADATA Table is a custom addition to the OMOP Vocabulary Infrastructure and stores the metadata associated with the scrape of a given concept.
#' @importFrom pg13 writeTable
#' @importFrom pag13 lsTables
#' @import dplyr
#' @export

createMetadataTable <-
    function(conn,
             .input) {

                if (nrow(.input) == 0) {
                    stop("input is empty")
                }

                cgTables <- pg13::lsTables(conn = conn,
                                           schema = "cancergov")


                if (!("concept_metadata" %in% tolower(cgTables))) {
                    pg13::writeTable(conn = conn,
                                     schema = "cancergov",
                                     tableName = "concept_metadata",
                                     .data = .input %>%
                                         dplyr::mutate(concept_timestamp = Sys.time()) %>%
                                         dplyr::select(concept_timestamp,
                                                       concept_id,
                                                       concept_page = page,
                                                       concept_page_url = page_url) %>%
                                         as.data.frame())
                }

    }



