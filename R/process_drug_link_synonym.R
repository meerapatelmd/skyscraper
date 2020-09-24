#' @title
#' Process the Links found in the Drug Link Table for Synonyms
#'
#' @inherit cancergov_functions description
#' @inheritSection cancergov_functions Web Source Types
#' @inheritSection cancergov_functions Drug Detail Links
#' @inheritParams cancergov_functions
#' @param sleep_time Seconds in between xml2::read_html calls on a URL, Default: 5
#'
#' @seealso
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{readTable}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{filter}},\code{\link[dplyr]{select}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{filter_all}}
#'  \code{\link[police]{try_catch_error_as_null}}
#'  \code{\link[xml2]{read_xml}},\code{\link[xml2]{xml_find_all}},\code{\link[xml2]{xml_replace}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#'  \code{\link[tidyr]{separate_rows}}
#' @family cancergov
#' @export
#' @importFrom pg13 lsTables query buildQuery readTable appendTable writeTable
#' @importFrom dplyr left_join filter select bind_rows transmute filter_at
#' @importFrom police try_catch_error_as_null
#' @importFrom xml2 read_html xml_find_all xml_add_sibling xml_remove
#' @importFrom rvest html_nodes html_table
#' @importFrom tidyr separate_rows
#' @importFrom magrittr %>%

process_drug_link_synonym <-
    function(conn,
             sleep_time = 5) {



            cgTables <- pg13::lsTables(conn = conn,
                                       schema = "cancergov")


            if ("DRUG_LINK_SYNONYM" %in% cgTables) {

                    drug_link_table <-
                    pg13::query(conn = conn,
                                sql_statement =
                                        "
                                        SELECT DISTINCT
                                                dl.drug, dl.drug_link
                                        FROM cancergov.drug_link dl
                                        LEFT JOIN cancergov.drug_link_synonym dls
                                        ON dls.drug_link  = dl.drug_link
                                        WHERE dls_datetime IS NULL
                                        ")


            } else {
                    drug_link_table <-
                            pg13::readTable(conn = conn,
                                            schema = "cancergov",
                                            tableName = "drug_link")
            }



            drug_links <- drug_link_table$drug_link


            while (length(drug_links)) {


                    #drug_link <- "https://www.cancer.gov/publications/dictionaries/cancer-drug/def/792667"

                    drug_link <- drug_links[1]


                    response <-
                            police::try_catch_error_as_null(
                                xml2::read_html(drug_link)
                            )

                    Sys.sleep(sleep_time)

                    if (is.null(response)) {

                            response <-
                                    police::try_catch_error_as_null(
                                            xml2::read_html(drug_link)
                                    )


                            Sys.sleep(sleep_time)

                    }



                    if (!is.null(response)) {


                            xml2::xml_find_all(response, ".//br") %>% xml2::xml_add_sibling("p", "\n")

                            xml2::xml_find_all(response, ".//br") %>% xml2::xml_remove()


                            results <-
                                    police::try_catch_error_as_null(
                                    response %>%
                                            rvest::html_nodes("table") %>%
                                            rvest::html_table()
                                    )


                            if (length(results)) {

                                    output  <-
                                            dplyr::bind_rows(results) %>%
                                            tidyr::separate_rows(X2,
                                                                 sep = "\n") %>%
                                            dplyr::transmute(dls_datetime = Sys.time(),
                                                             drug_link = drug_link,
                                                             drug_synonym_type = X1,
                                                             drug_synonym = X2) %>%
                                            dplyr::filter_at(vars(drug_synonym_type,drug_synonym),
                                                             ~nchar(.) < 255)



                                    cgTables <- pg13::lsTables(conn = conn,
                                                              schema = "cancergov")

                                    if ("DRUG_LINK_SYNONYM" %in% cgTables) {

                                            pg13::appendTable(conn = conn,
                                                              schema = "cancergov",
                                                              tableName = "drug_link_synonym",
                                                              output)


                                    } else {
                                            pg13::writeTable(conn = conn,
                                                             schema = "cancergov",
                                                             tableName = "drug_link_synonym",
                                                             output)
                                    }


                            } else {
                                    output <-
                                            data.frame(dls_datetime = Sys.time(),
                                                       drug_link = drug_link,
                                                       drug_synonym_type = NA,
                                                       drug_synonym = NA)

                                    cgTables <- pg13::lsTables(conn = conn,
                                                               schema = "cancergov")

                                    if ("DRUG_LINK_SYNONYM" %in% cgTables) {

                                            pg13::appendTable(conn = conn,
                                                              schema = "cancergov",
                                                              tableName = "drug_link_synonym",
                                                              output)


                                    } else {
                                            pg13::writeTable(conn = conn,
                                                             schema = "cancergov",
                                                             tableName = "drug_link_synonym",
                                                             output)
                                    }
                            }

                    }


                    drug_links <- drug_links[-1]


            }

    }

