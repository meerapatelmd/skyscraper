#' @title
#' Scrape the Drug Definitions and Links from the NCI Drug Dictionary
#'
#' @inherit cancergov_functions description
#' @inheritSection cancergov_functions Web Source Types
#' @inheritSection cancergov_functions Drug Dictionary
#' @inheritParams cancergov_functions
#'
#' @details
#' This function combines the operations of \code{\link{get_drug_detail_links}} and \code{\link{get_drug_dictionary}} to parse from a single response.
#'
#' @return
#' Drug Dictionary and Drug Link Table in the `cancergov` schema if a Drug Dictionary Table didn't already exist. Otherwise, both Tables are appended with any new observations scraped.
#'
#' @seealso
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[secretary]{typewrite}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{keep}},\code{\link[purrr]{transpose}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate_all}}
#'  \code{\link[stringr]{str_replace}}
#'
#' @export
#' @rdname get_dictionary_and_links
#' @family cancergov
#'
#' @importFrom tibble tibble
#' @importFrom secretary typewrite
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_text
#' @importFrom purrr map keep transpose
#' @importFrom dplyr bind_rows mutate_all
#' @importFrom stringr str_replace_all
#' @importFrom magrittr %>%



get_dictionary_and_links <-
    function(conn,
             max_page = 50,
             sleep_time = 3,
             progress_bar = TRUE) {

        drug_dictionary <- list()
        drug_link <- list()

        if (progress_bar) {

                pb <- progress::progress_bar$new(format = "[:bar] :current/:total :elapsedfull",
                                                 total = max_page)

                pb$tick(0)
                Sys.sleep(0.2)


        }


        for (i in 1:max_page) {


                page_scrape <- xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

                if (progress_bar) {

                        pb$tick()


                }


                Sys.sleep(sleep_time)

                no_data_message <-
                        page_scrape %>%
                        rvest::html_nodes("#ctl36_ctl00_resultListView_ctrl0_lblNoDataMessage") %>%
                        rvest::html_text()

                if (length(no_data_message) == 0) {

                                drugs <-
                                page_scrape %>%
                                        rvest::html_nodes(".dictionary-list a")  %>%
                                        rvest::html_text() %>%
                                        grep(pattern = "[\r\n]",
                                             invert = FALSE,
                                             value = TRUE) %>%
                                        trimws(which = "both")

                                definitions <-
                                page_scrape %>%
                                        rvest::html_nodes(".dictionary-list .definition")  %>%
                                        rvest::html_text() %>%
                                        trimws(which = "both")



                                drug_dictionary[[i]] <-
                                       tibble::tibble(drug = drugs,
                                                  definition = definitions)

                                drug_def_link <-
                                        page_scrape %>%
                                        rvest::html_nodes("dfn") %>%
                                        rvest::html_children() %>%
                                        rvest::html_attr(name = "href")

                                drug_names <-
                                        page_scrape %>%
                                        rvest::html_nodes("dfn") %>%
                                        rvest::html_text() %>%
                                        trimws()

                                drug_link[[i]] <-
                                        tibble::tibble(drug= drug_names,
                                                       drug_link = drug_def_link)


                        }
        }

                drug_dictionary_table <-
                        dplyr::bind_rows(drug_dictionary)

                drug_link_table <-
                        dplyr::bind_rows(drug_link) %>%
                        dplyr::mutate(drug_link = paste0("https://www.cancer.gov", drug_link)) %>%
                        dplyr::distinct()



                cgTables <- pg13::lsTables(conn = conn,
                                               schema = "cancergov")


            if ("DRUG_DICTIONARY" %in% cgTables) {

                    pg13::dropTable(conn = conn,
                                    schema = "cancergov",
                                    tableName = "new_drug_dictionary")

                    pg13::writeTable(conn = conn,
                                    schema = "cancergov",
                                    tableName = "new_drug_dictionary",
                                    drug_dictionary_table)


                    add_to_drug_dictionary <-
                            pg13::query(conn = conn,
                                        sql_statement =
                                                "SELECT ndd.drug, ndd.definition
                                                FROM cancergov.new_drug_dictionary ndd
                                                LEFT JOIN cancergov.drug_dictionary dd
                                                ON dd.drug = ndd.drug
                                                        AND dd.definition = ndd.definition
                                                WHERE dd_datetime IS NULL;") %>%
                            dplyr::transmute(dd_datetime = Sys.time(),
                                             drug,
                                             definition) %>%
                            dplyr::distinct()


                    pg13::appendTable(conn = conn,
                                      schema = "cancergov",
                                      tableName = "drug_dictionary",
                                      add_to_drug_dictionary)

                    pg13::dropTable(conn = conn,
                                    schema = "cancergov",
                                    tableName = "new_drug_dictionary")

            } else {

                    pg13::writeTable(conn = conn,
                                     schema = "cancergov",
                                     tableName = "drug_dictionary",
                                     drug_dictionary_table %>%
                                             dplyr::transmute(dd_datetime = Sys.time(),
                                                              drug,
                                                              definition) %>%
                                             dplyr::distinct())
            }



                if ("DRUG_LINK" %in% cgTables) {

                        pg13::dropTable(conn = conn,
                                        schema = "cancergov",
                                        tableName = "new_drug_link")

                        pg13::writeTable(conn = conn,
                                         schema = "cancergov",
                                         tableName = "new_drug_link",
                                         drug_link_table)


                        add_to_drug_link <-
                                pg13::query(conn = conn,
                                            sql_statement =
                                                    "SELECT ndl.drug, ndl.drug_link
                                                FROM cancergov.new_drug_link ndl
                                                LEFT JOIN cancergov.drug_link dl
                                                ON dl.drug = ndl.drug
                                                        AND dl.drug_link = ndl.drug_link
                                                WHERE dl_datetime IS NULL;") %>%
                                dplyr::transmute(dl_datetime = Sys.time(),
                                                 drug,
                                                 drug_link) %>%
                                dplyr::distinct()

                        pg13::appendTable(conn = conn,
                                          schema = "cancergov",
                                          tableName = "drug_link",
                                          add_to_drug_link)

                        pg13::dropTable(conn = conn,
                                        schema = "cancergov",
                                        tableName = "new_drug_link")


                } else {
                        pg13::writeTable(conn = conn,
                                         schema = "cancergov",
                                         tableName = "drug_link",
                                         drug_link_table %>%
                                                 dplyr::transmute(dl_datetime = Sys.time(),
                                                                  drug,
                                                                  drug_link) %>%
                                                 dplyr::distinct())
                }

    }
