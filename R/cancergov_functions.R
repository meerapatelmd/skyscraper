#' @title
#' CancerGov Functions
#'
#' @description
#' These functions scrape and parse the NCI Drug Dictionary found at CancerGov.org.
#'
#' @section
#' Web Source Types:
#' The NCI Drug Dictionary has 2 data sources that run in parallel. The first source is the Drug Dictionary itself at \href{https://www.cancer.gov/publications/dictionaries/cancer-drug}{https://www.cancer.gov/publications/dictionaries/cancer-drug}. The other source are the individual drug pages, called Drug Detail Links in skyscraper, that contain tables of synonyms, including investigational names.
#'
#' @section
#' Drug Dictionary:
#' The listed drug names and their definitions are scraped from the Drug Dictionary HTML and updated to a Drug Dictionary Table in a `cancergov` schema.
#'
#' @section
#' Drug Detail Links:
#' The links to Drug Pages are scraped from the Data Dictionary URL over the maximum page number and are saved to a Drug Link Table in the `cancergov` schema. The URLs in the Drug Link Table are then scraped for any HTML Tables of synonyms and the results are written to a Drug Link Synonym Table. The links to active clinical trials and NCIt mappings are also derived and stored in their respective tables.
#'
#'
#' @param conn Postgres connection object
#' @param max_page maximum page number to iterate the scrape over in the "https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=" path, Default: 50
#'
#'
#' .
#'
#' @name cancergov_functions
NULL


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




#' @title
#' Get the Synonyms found at a given Drug Link
#'
#' @inherit cancergov_functions description
#' @inheritSection cancergov_functions Web Source Types
#' @inheritSection cancergov_functions Drug Detail Links
#' @inheritParams cancergov_functions
#' @param sleep_time Seconds in between xml2::read_html calls on a URL, Default: 5
#' @param drug_link Drug Link
#' @param response (optional) The response associated with the `drug_link`. If not provided, a new response is read.
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

get_drug_link_synonym <-
        function(conn,
                 drug_link,
                 response,
                 sleep_time = 5) {



                cgTables <- pg13::lsTables(conn = conn,
                                           schema = "cancergov")


                if ("DRUG_LINK_SYNONYM" %in% cgTables) {

                        current_entry <-
                                pg13::query(conn = conn,
                                            sql_statement =
                                                    SqlRender::render(
                                                            "
                                                        SELECT *
                                                        FROM cancergov.drug_link_synonym dls
                                                        WHERE dls.drug_link = '@drug_link'
                                                        ;
                                                        ",
                                                            drug_link = drug_link))


                        proceed <- nrow(current_entry) == 0


                } else {

                        proceed <- TRUE
                }

                if (proceed) {


                        if (missing(response)) {

                                response <-
                                        police::try_catch_error_as_null(
                                                xml2::read_html(drug_link)
                                        )


                                if (is.null(response)) {

                                        Sys.sleep(sleep_time)

                                        response <-
                                                police::try_catch_error_as_null(
                                                        xml2::read_html(drug_link)
                                                )


                                        Sys.sleep(sleep_time)

                                } else {

                                        Sys.sleep(sleep_time)
                                }
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

                }
        }




#' @title
#' Get the URLS of all the Drug Pages in the Drug Dictionary
#'
#' @inherit cancergov_functions description
#' @inheritSection cancergov_functions Web Source Types
#' @inheritSection cancergov_functions Drug Detail Links
#' @inheritParams cancergov_functions
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{filter}}
#'  \code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'
#' @rdname get_drug_detail_links
#'
#' @family cancergov
#'
#' @export
#'
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_text html_children html_attr
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows mutate transmute distinct left_join filter
#' @importFrom pg13 lsTables query buildQuery appendTable writeTable
#' @importFrom magrittr %>%

get_drug_detail_links  <-
        function(conn,
                 max_page = 50,
                 sleep_time = 3) {


                .Deprecated()

                output <- list()

                for (i in 1:max_page) {

                        drug_def_scrape <- xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

                        Sys.sleep(sleep_time)

                        # Stop if there is no more information
                        no_data_message <-
                                drug_def_scrape %>%
                                rvest::html_nodes("#ctl36_ctl00_resultListView_ctrl0_lblNoDataMessage") %>%
                                rvest::html_text()


                        if (length(no_data_message) == 0) {


                                drug_def_link <-
                                        drug_def_scrape %>%
                                        rvest::html_nodes("dfn") %>%
                                        rvest::html_children() %>%
                                        rvest::html_attr(name = "href")

                                drug_names <-
                                        drug_def_scrape %>%
                                        rvest::html_nodes("dfn") %>%
                                        rvest::html_text() %>%
                                        trimws()

                                output[[i]] <- tibble::tibble(DRUG = drug_names,
                                                              DRUG_DEF_LINK = drug_def_link)
                                names(output)[i] <- as.character(i)

                        }

                }

                drug_link_table <- dplyr::bind_rows(output) %>%
                        dplyr::mutate(DRUG_DEF_LINK = paste0("https://www.cancer.gov", DRUG_DEF_LINK)) %>%
                        dplyr::transmute(drug = DRUG,
                                         drug_link = DRUG_DEF_LINK) %>%
                        dplyr::distinct()



                cgTables <- pg13::lsTables(conn = conn,
                                           schema = "cancergov")


                if ("DRUG_LINK" %in% cgTables) {
                        current_drug_link_table <-
                                pg13::query(conn = conn,
                                            pg13::buildQuery(distinct = TRUE,
                                                             schema = "cancergov",
                                                             tableName = "drug_link"))
                        output <-
                                dplyr::left_join(drug_link_table,
                                                 current_drug_link_table) %>%
                                dplyr::filter(is.na(dl_datetime)) %>%
                                dplyr::transmute(dl_datetime = Sys.time(),
                                                 drug,
                                                 drug_link)


                        pg13::appendTable(conn = conn,
                                          schema = "cancergov",
                                          tableName = "drug_link",
                                          output)


                } else {
                        pg13::writeTable(conn = conn,
                                         schema = "cancergov",
                                         tableName = "drug_link",
                                         drug_link_table %>%
                                                 dplyr::transmute(dl_datetime = Sys.time(),
                                                                  drug,
                                                                  drug_link))
                }

        }


#' @title
#' Get the URLs found in a Drug Link
#'
#' @inherit cancergov_functions description
#' @inheritSection cancergov_functions Web Source Types
#' @inheritSection cancergov_functions Drug Detail Links
#' @inheritParams cancergov_functions
#' @param sleep_time Seconds in between xml2::read_html calls on a URL, Default: 3
#' @param drug_link Drug Link
#' @param response (optional) The response associated with the `drug_link`. If not provided, a new response is read.
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

get_drug_link_url <-
        function(conn,
                 drug_link,
                 response,
                 sleep_time = 5) {

                #conn <- chariot::connectAthena()

                # drug_link <- "https://www.cancer.gov/publications/dictionaries/cancer-drug/def/cd105-yb-1-sox2-cdh3-mdm2-polypeptide-plasmid-dna-vaccine"

                cgTables <- pg13::lsTables(conn = conn,
                                           schema = "cancergov")


                if ("DRUG_LINK_URL" %in% cgTables) {

                        current_entry <-
                                pg13::query(conn = conn,
                                            sql_statement =
                                                    SqlRender::render(
                                                            "
                                                        SELECT *
                                                        FROM cancergov.drug_link_url dlu
                                                        WHERE dlu.drug_link = '@drug_link'
                                                        ;
                                                        ",
                                                            drug_link = drug_link))


                        proceed <- nrow(current_entry) == 0


                } else {

                        proceed <- TRUE

                }


                if (proceed) {


                        if (missing(response)) {

                                response <-
                                        police::try_catch_error_as_null(
                                                xml2::read_html(drug_link)
                                        )


                                if (is.null(response)) {

                                        Sys.sleep(sleep_time)

                                        response <-
                                                police::try_catch_error_as_null(
                                                        xml2::read_html(drug_link)
                                                )


                                        Sys.sleep(sleep_time)

                                } else {

                                        Sys.sleep(sleep_time)
                                }
                        }

                        if (!is.null(response)) {

                                results <-
                                        police::try_catch_error_as_null(
                                                response %>%
                                                        rvest::html_nodes(".navigation-dark-red") %>%
                                                        rvest::html_attr("href")
                                        )



                                if (length(results)) {

                                        output  <-
                                                tibble::tibble(dlu_datetime = Sys.time(),
                                                               drug_link = drug_link,
                                                               drug_link_url = results)



                                        cgTables <- pg13::lsTables(conn = conn,
                                                                   schema = "cancergov")

                                        if ("DRUG_LINK_URL" %in% cgTables) {

                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "DRUG_LINK_URL",
                                                                  output)


                                        } else {
                                                pg13::writeTable(conn = conn,
                                                                 schema = "cancergov",
                                                                 tableName = "DRUG_LINK_URL",
                                                                 output)
                                        }


                                        process_drug_link_ncit(conn = conn)


                                } else {

                                        output  <-
                                                tibble::tibble(dlu_datetime = Sys.time(),
                                                               drug_link = drug_link,
                                                               drug_link_url = NA)

                                        cgTables <- pg13::lsTables(conn = conn,
                                                                   schema = "cancergov")

                                        if ("DRUG_LINK_URL" %in% cgTables) {

                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "DRUG_LINK_URL",
                                                                  output)


                                        } else {
                                                pg13::writeTable(conn = conn,
                                                                 schema = "cancergov",
                                                                 tableName = "DRUG_LINK_URL",
                                                                 output)
                                        }
                                }

                        }

                }
        }







#' @title
#' Scrape the NCI Drug Dictionary
#'
#' @inherit cancergov_functions description
#' @inheritSection cancergov_functions Web Source Types
#' @inheritSection cancergov_functions Drug Dictionary
#' @inheritParams cancergov_functions
#'
#' @return
#' A Drug Dictionary Table in a `cancergov` schema if a Drug Dictionary Table didn't already exist. Otherwise, the new drugs are appended to the existing table.
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
#' @rdname get_drug_dictionary
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



get_drug_dictionary <-
        function(conn,
                 max_page = 50,
                 sleep_time = 3) {

                .Deprecated()

                output <- list()

                for (i in 1:max_page) {


                        page_scrape <- xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

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


                                output[[i]] <-
                                        data.frame(drug = drugs,
                                                   definition = definitions)



                        }
                }

                output <- dplyr::bind_rows(output) %>%
                        dplyr::distinct()


                cgTables <- pg13::lsTables(conn = conn,
                                           schema = "cancergov")


                if ("DRUG_DICTIONARY" %in% cgTables) {

                        current_drug_dictionary <- pg13::readTable(conn = conn,
                                                                   schema = "cancergov",
                                                                   tableName = "drug_dictionary")


                        new_drug_dictionary <-
                                dplyr::left_join(output,
                                                 current_drug_dictionary,
                                                 by = c("drug", "definition")) %>%
                                dplyr::filter(is.na(dd_datetime)) %>%
                                dplyr::transmute(dd_datetime = Sys.time(),
                                                 drug,
                                                 definition) %>%
                                dplyr::distinct()


                        pg13::appendTable(conn = conn,
                                          schema = "cancergov",
                                          tableName = "drug_dictionary",
                                          new_drug_dictionary )



                } else {

                        pg13::writeTable(conn = conn,
                                         schema = "cancergov",
                                         tableName = "drug_dictionary",
                                         output %>%
                                                 dplyr::transmute(dd_datetime = Sys.time(),
                                                                  drug,
                                                                  definition))
                }

        }
