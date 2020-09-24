#' @title
#' Process the NCIt CUI from the Drug Link URL Table
#'
#' @inherit cancergov_functions description
#' @inheritSection cancergov_functions Web Source Types
#' @inheritSection cancergov_functions Drug Detail Links
#' @details
#' This function parses the NCI Thesaurus CUI from the scraped URL.
#' @inheritParams cancergov_functions
#' @rdname process_drug_link_ncit
#' @family cancergov
#' @seealso
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{readTable}},\code{\link[pg13]{dropTable}},\code{\link[pg13]{writeTable}},\code{\link[pg13]{appendTable}}
#'  \code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{filter}},\code{\link[dplyr]{select}},\code{\link[dplyr]{mutate}}
#'  \code{\link[rubix]{filter_at_grepl}}
#'  \code{\link[tidyr]{extract}}
#' @export
#' @importFrom pg13 lsTables query buildQuery readTable dropTable writeTable appendTable
#' @importFrom dplyr left_join filter select transmute
#' @importFrom rubix filter_at_grepl
#' @importFrom tidyr extract


process_drug_link_ncit <-
    function(conn) {

                #conn <- chariot::connectAthena()


            cgTables <- pg13::lsTables(conn = conn,
                                       schema = "cancergov")


            if ("DRUG_LINK_NCIT" %in% cgTables) {
                    current_drug_link_ncit <-
                            pg13::query(conn = conn,
                                        pg13::buildQuery(distinct = TRUE,
                                                         schema = "cancergov",
                                                         tableName = "DRUG_LINK_NCIT"))

                    drug_link_table0 <-
                            pg13::readTable(conn = conn,
                                            schema = "cancergov",
                                            tableName = "drug_link")


                    drug_link_table <-
                            dplyr::left_join(drug_link_table0,
                                             current_drug_link_ncit,
                                             by = "drug_link") %>%
                            dplyr::filter(is.na(dln_datetime)) %>%
                            dplyr::select(all_of(colnames(drug_link_table0)))


            } else {
                    drug_link_table <-
                            pg13::readTable(conn = conn,
                                            schema = "cancergov",
                                            tableName = "drug_link")
            }


            pg13::dropTable(conn = conn,
                             schema = 'cancergov',
                             tableName = "temp_drug_link_table")

            pg13::writeTable(conn = conn,
                             schema = 'cancergov',
                             tableName = "temp_drug_link_table",
                             drug_link_table)

            drug_link_url_table <-
                    pg13::query(conn = conn,
                                sql_statement =
                                        "
                                        SELECT DISTINCT
                                                dlu.drug_link, dlu.drug_link_url
                                        FROM cancergov.temp_drug_link_table temp
                                        INNER JOIN cancergov.drug_link_url dlu
                                        ON dlu.drug_link = temp.drug_link
                                        ;
                                        "
                                )

            pg13::dropTable(conn = conn,
                            schema = 'cancergov',
                            tableName = "temp_drug_link_table")


            results <-
                        drug_link_url_table %>%
                        rubix::filter_at_grepl(drug_link_url,
                                               grepl_phrase = "ncit.nci.nih.gov") %>%
                        tidyr::extract(drug_link_url,
                                       into = "ncit_code",
                                       regex = "^.*?code=(.*$)") %>%
                        dplyr::transmute(dln_datetime = Sys.time(),
                                         drug_link,
                                         ncit_code)


            cgTables <- pg13::lsTables(conn = conn,
                                       schema = "cancergov")

            if ("DRUG_LINK_NCIT" %in% cgTables) {

                    pg13::appendTable(conn = conn,
                                      schema = "cancergov",
                                      tableName = "DRUG_LINK_NCIT",
                                      results)


            } else {
                    pg13::writeTable(conn = conn,
                                     schema = "cancergov",
                                     tableName = "DRUG_LINK_NCIT",
                                     results)
            }

    }

