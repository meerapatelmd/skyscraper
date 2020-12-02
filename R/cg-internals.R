#' @title
#' CancerGov Internal Functions
#'
#' @description
#' This is an internal function to the `_run*` function, which is part of the family of functions that scrape, parse, and store the NCI Drug Dictionary found at CancerGov.org and any correlates to the NCI Thesaurus in a Postgres Database. Use \code{\link{cg_run}} to run the full sequence. See details for more info.
#'
#' @name cancergov_internal
NULL

#' @title
#' Get the Drug Count in the Drug Dictionary
#'
#' @inherit cancergov_internal description
#'
#' @details
#' Retrieve the total number of drugs in the NCI Drug Dictionary in real-time.
#'
#' @return
#' An integer in the '\emph{X} results found for: ALL' phrase displayed at \url{https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=1}
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#' @rdname drug_count
#' @export
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_text
#' @importFrom magrittr %>%

drug_count <-
        function() {
                page_scrape <-
                        xml2::read_html("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=1")

                page_scrape %>%
                        rvest::html_nodes("#ctl36_ctl00_lblNumResults") %>%
                        rvest::html_text() %>%
                        as.integer()

        }


#' @title
#' Log the Drug Count in the Drug Dictionary
#'
#' @description
#' Log the drug count scraped by \code{\link{drug_count}} with a timestamp to the Drug Dictionary Log Table and also receive a time difference and count comparison between the most recent count and the current count in the R console.
#'
#' @inheritParams cg_run
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[secretary]{typewrite}}
#'  \code{\link[tibble]{tibble}}
#' @rdname log_drug_count
#' @export
#' @importFrom pg13 query appendTable
#' @importFrom secretary typewrite
#' @importFrom tibble tibble


log_drug_count <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE) {


                nci_dd_count <- drug_count()

                most_recent_count <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                                "
                                                SELECT ddl.ddl_datetime, ddl.drug_count
                                                FROM cancergov.drug_dictionary_log ddl
                                                WHERE ddl.ddl_datetime IN (
                                                                SELECT MAX(ddl_datetime)
                                                                FROM cancergov.drug_dictionary_log
                                                )
                                                "
                                            )

                if (verbose) {
                        time_difference <- as.character(signif(Sys.time()-most_recent_count$ddl_datetime, digits = 3))
                        time_difference_units <- units(Sys.time()-most_recent_count$ddl_datetime)
                        secretary::typewrite(sprintf("Last drug count was %s and from %s %s ago",
                                                                most_recent_count$drug_count,
                                                                time_difference,
                                                                time_difference_units))

                        secretary::typewrite("Current drug count:", nci_dd_count)
                }


                pg13::appendTable(conn = conn,
                                  schema = "cancergov",
                                  tableName = "DRUG_DICTIONARY_LOG",
                                  data = tibble::tibble(ddl_datetime = Sys.time(),
                                                 drug_count = nci_dd_count))


        }


#' @title
#' Scrape the Drug Definitions and Links from the NCI Drug Dictionary
#'
#' @inherit cg_run description
#' @inheritSection cg_run Web Source Types
#' @inheritSection cg_run Drug Dictionary
#' @inheritParams cg_run
#'
#' @details
#' Scrapes the Definitions and the links to each Drug Page at the main Drug Dictionary pages in  the \url{https://www.cancer.gov/publications/dictionaries/cancer-drug}\emph{{i}} and stores the parsed response to the Drug Dictionary and Drug Link Tables, respectively.
#'
#' @return
#' Any differences found between the scraped data and the existing data in the Drug Dictionary and Drug Link Tables are appended to their respective tables with the local timestamp.
#'
#'
#' @seealso
#'  \code{\link[pg13]{brake_closed_conn}},\code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[secretary]{typewrite_progress}},\code{\link[secretary]{c("typewrite", "typewrite")}},\code{\link[secretary]{character(0)}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{distinct}}
#' @rdname get_dictionary_and_links
#' @export
#' @importFrom pg13 brake_closed_conn query appendTable
#' @importFrom secretary typewrite_progress typewrite silverTxt
#' @importFrom rvest html_nodes html_text html_children html_attr
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows mutate distinct transmute



get_dictionary_and_links <-
        function(conn,
                 max_page = 50,
                 sleep_time = 3,
                 verbose = TRUE,
                 render_sql = TRUE) {

                pg13::brake_closed_conn(conn = conn)

                drug_dictionary <- list()
                drug_link <- list()

                for (i in 1:max_page) {

                        page_url <- sprintf("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=%s", i)

                        if (verbose) {

                                secretary::typewrite_progress(iteration = i,
                                                              total = max_page)
                                secretary::typewrite(secretary::silverTxt("Page:"), i)


                        }

                        page_scrape <- scrape(page_url)


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


                if (verbose) {
                        secretary::typewrite("Updating DRUG_DICTIONARY Table with diffs")
                }

                        write_cg_staging_tbl(conn = conn,
                                             tableName = "drug_dictionary_staging",
                                             data = drug_dictionary_table,
                                             verbose = verbose,
                                             render_sql = render_sql)


                        add_to_drug_dictionary <-
                                pg13::query(conn = conn,
                                            sql_statement =
                                                    "SELECT ndd.drug, ndd.definition
                                                FROM cancergov.drug_dictionary_staging ndd
                                                LEFT JOIN cancergov.drug_dictionary dd
                                                ON dd.drug = ndd.drug
                                                        AND dd.definition = ndd.definition
                                                WHERE dd_datetime IS NULL;",
                                            warn_no_rows = TRUE) %>%
                                dplyr::transmute(dd_datetime = Sys.time(),
                                                 drug,
                                                 definition) %>%
                                dplyr::distinct()


                        pg13::appendTable(conn = conn,
                                          schema = "cancergov",
                                          tableName = "drug_dictionary",
                                          data = add_to_drug_dictionary)


                        if (verbose) {
                                secretary::typewrite("Updating DRUG_LINK Table with diffs")
                        }

                        write_cg_staging_tbl(conn = conn,
                                             tableName = "new_drug_link",
                                             data = drug_link_table,
                                             verbose = verbose,
                                             render_sql = render_sql)

                        add_to_drug_link <-
                                pg13::query(conn = conn,
                                            sql_statement =
                                                    "SELECT ndl.drug, ndl.drug_link
                                                FROM cancergov.new_drug_link ndl
                                                LEFT JOIN cancergov.drug_link dl
                                                ON dl.drug = ndl.drug
                                                        AND dl.drug_link = ndl.drug_link
                                                WHERE dl_datetime IS NULL;",
                                            warn_no_rows = TRUE) %>%
                                dplyr::transmute(dl_datetime = Sys.time(),
                                                 drug,
                                                 drug_link) %>%
                                dplyr::distinct()

                        pg13::appendTable(conn = conn,
                                          schema = "cancergov",
                                          tableName = "drug_link",
                                          data = add_to_drug_link)


        }




#' @title
#' Get the Synonyms found at a given Drug Link
#'
#' @inherit cg_run description
#' @inheritSection cg_run Drug Detail Links
#' @inheritParams cg_run
#' @param response (Optional) The response returned when the url supplied as the `drug_link` is parsed. If not provided, a new response is received. This is an option to reduce repetitive scrapes of the same URL if multiple sections of the same page are being parsed.
#'
#' @seealso
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[police]{try_catch_error_as_null}}
#'  \code{\link[xml2]{read_xml}},\code{\link[xml2]{xml_find_all}},\code{\link[xml2]{xml_replace}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{filter_all}}
#'  \code{\link[tidyr]{separate_rows}}
#' @rdname get_drug_link_synonym
#' @export
#' @importFrom SqlRender render
#' @importFrom pg13 query appendTable
#' @importFrom police try_catch_error_as_null
#' @importFrom xml2 read_html xml_find_all xml_add_sibling xml_remove
#' @importFrom rvest html_nodes html_table
#' @importFrom dplyr bind_rows transmute filter_at
#' @importFrom tidyr separate_rows

get_drug_link_synonym <-
        function(conn,
                 drug_link,
                 response,
                 sleep_time = 5,
                 expiration_days = 30,
                 verbose = TRUE,
                 render_sql = TRUE) {

                # drug_link <- "https://www.cancer.gov/publications/dictionaries/cancer-drug/def/bispecific-antibody-mdx-h210"

                sql_statement <-
                        SqlRender::render(
                                "
                                SELECT *
                                        FROM cancergov.drug_link_synonym dls
                                WHERE dls.drug_link = '@drug_link'
                                        AND (DATE_PART('day', LOCALTIMESTAMP(0)-dls_datetime)::integer < @expiration_days)
                                ;
                                ",
                                drug_link = drug_link,
                                expiration_days = expiration_days)

                current_entry <-
                pg13::query(conn = conn,
                            sql_statement = sql_statement,
                            verbose = verbose,
                            render_sql = render_sql)

                proceed <- nrow(current_entry) == 0


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


                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "drug_link_synonym",
                                                                  data = output)




                                } else {
                                        output <-
                                                data.frame(dls_datetime = Sys.time(),
                                                           drug_link = drug_link,
                                                           drug_synonym_type = NA,
                                                           drug_synonym = NA)

                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "drug_link_synonym",
                                                                  output)

                                }

                        }

                }
        }


#' @title
#' Get the URLs found in a Drug Link
#'
#' @inherit cg_run description
#' @inheritSection cg_run Drug Detail Links
#' @inheritParams get_drug_link_synonym
#'
#' @seealso
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[police]{try_catch_error_as_null}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#' @rdname get_drug_link_url
#' @export
#' @importFrom SqlRender render
#' @importFrom pg13 query appendTable
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_attr
#' @importFrom tibble tibble

get_drug_link_url <-
        function(conn,
                 drug_link,
                 response,
                 sleep_time = 5,
                 expiration_days = 30,
                 verbose = TRUE,
                 render_sql = TRUE) {

                sql_statement <-
                        SqlRender::render(
                                "
                                SELECT *
                                        FROM cancergov.drug_link_url dlu
                                WHERE dlu.drug_link = '@drug_link'
                                        AND (DATE_PART('day', LOCALTIMESTAMP(0)-dlu_datetime)::integer < @expiration_days)
                                ;
                                ",
                                drug_link = drug_link,
                                expiration_days = expiration_days)

                current_entry <-
                        pg13::query(conn = conn,
                                    sql_statement = sql_statement,
                                    verbose = verbose,
                                    render_sql = render_sql)

                proceed <- nrow(current_entry) == 0

                if (proceed) {


                        if (missing(response)) {

                                response <- scrape(drug_link)

                        }

                        if (!is.null(response)) {

                                results <-
                                        tryCatch(
                                                response %>%
                                                        rvest::html_nodes(".navigation-dark-red") %>%
                                                        rvest::html_attr("href"),
                                                error = function(e) NULL
                                        )



                                if (length(results)) {

                                        output  <-
                                                tibble::tibble(dlu_datetime = Sys.time(),
                                                               drug_link = drug_link,
                                                               drug_link_url = results)


                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "DRUG_LINK_URL",
                                                                  data = output)

                                } else {

                                        output  <-
                                                tibble::tibble(dlu_datetime = Sys.time(),
                                                               drug_link = drug_link,
                                                               drug_link_url = NA)

                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "DRUG_LINK_URL",
                                                                  data = output)

                                }

                        }

                }
        }

#' @title
#' Process the Links found in the Drug Link Table for Synonyms
#' @inherit cg_run description
#' @inheritSection cg_run Drug Detail Links
#' @inheritParams get_drug_link_synonym
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[secretary]{typewrite_progress}},\code{\link[secretary]{c("typewrite", "typewrite")}},\code{\link[secretary]{character(0)}}
#'  \code{\link[xml2]{xml_find_all}},\code{\link[xml2]{xml_replace}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{filter_all}}
#'  \code{\link[tidyr]{separate_rows}}
#'  \code{\link[tibble]{tibble}}
#' @rdname process_drug_link_synonym
#' @export
#' @importFrom pg13 query appendTable
#' @importFrom SqlRender render
#' @importFrom secretary typewrite_progress typewrite magentaTxt
#' @importFrom xml2 xml_find_all xml_add_sibling xml_remove
#' @importFrom rvest html_nodes html_table
#' @importFrom dplyr bind_rows transmute filter_at
#' @importFrom tidyr separate_rows
#' @importFrom tibble tibble


process_drug_link_synonym <-
        function(conn,
                 sleep_time = 3,
                 expiration_days = 30,
                 verbose = TRUE,
                 render_sql = TRUE,
                 encoding = "",
                 options = c("RECOVER", "NOERROR", "NOBLANKS")) {

                        drug_link_table <-
                                pg13::query(conn = conn,
                                            sql_statement =

                                                    SqlRender::render(
                                                    "
                                        SELECT DISTINCT
                                                dl.drug, dl.drug_link
                                        FROM cancergov.drug_link dl
                                        LEFT JOIN cancergov.drug_link_synonym dls
                                        ON dls.drug_link  = dl.drug_link
                                        WHERE dls.dls_datetime IS NULL OR
                                                DATE_PART('day', dls.dls_datetime - LOCALTIMESTAMP(0))::integer >= @expiration_days
                                        ",
                                                    expiration_days = expiration_days),
                                            verbose = verbose,
                                            render_sql = render_sql)




                drug_links <- unique(drug_link_table$drug_link)


                for (i in seq_along(drug_links)) {


                        #drug_link <- "https://www.cancer.gov/publications/dictionaries/cancer-drug/def/792667"

                        drug_link <- drug_links[i]

                        if (verbose) {

                                secretary::typewrite_progress(iteration = i,
                                                              total = length(drug_links))
                                secretary::typewrite(secretary::magentaTxt("Drug Link:"), drug_link)
                        }


                        response <- scrape(drug_link,
                                           encoding = encoding,
                                           options = options,
                                           sleep_time = sleep_time,
                                           verbose = verbose)




                        if (!is.null(response)) {


                                xml2::xml_find_all(response, ".//br") %>% xml2::xml_add_sibling("p", "\n")

                                xml2::xml_find_all(response, ".//br") %>% xml2::xml_remove()


                                results <-
                                        tryCatch(
                                                response %>%
                                                        rvest::html_nodes("table") %>%
                                                        rvest::html_table(),
                                                error = function(e) NULL
                                        )


                                if (length(results) > 0) {

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

                                        pg13::appendTable(conn = conn,
                                                          schema = "cancergov",
                                                          tableName = "drug_link_synonym",
                                                          data = output)



                                } else {
                                        output <-
                                                tibble::tibble(dls_datetime = Sys.time(),
                                                           drug_link = drug_link,
                                                           drug_synonym_type = NA,
                                                           drug_synonym = NA)

                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "drug_link_synonym",
                                                                  data = output)


                                }


                        }


                }

        }

#' @title
#' Process the NCIt CUI from the Drug Link URL Table
#'
#' @inherit cg_run description
#' @inheritSection cg_run Drug Detail Links
#' @inheritParams get_drug_link_synonym
#'
#' @details
#' Unlike the other `process_*` functions, this one uses regex to find the NCI Thesaurus Code, if presence, from the scraped URLs instead of performing any scraping.
#'
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[rubix]{filter_at_grepl}}
#'  \code{\link[tidyr]{extract}}
#'  \code{\link[dplyr]{mutate}}
#' @rdname process_drug_link_ncit
#' @export
#' @importFrom pg13 query appendTable
#' @importFrom SqlRender render
#' @importFrom rubix filter_at_grepl
#' @importFrom tidyr extract
#' @importFrom dplyr transmute


process_drug_link_ncit <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE,
                 expiration_days = 30) {


                        drug_link_table <-
                                pg13::query(
                                        conn = conn,
                        SqlRender::render(
                        "SELECT dl.*
                        FROM cancergov.drug_link dl
                        LEFT JOIN cancergov.drug_link_ncit dln
                        ON dln.drug_link = dl.drug_link
                        WHERE dln.dln_datetime IS NULL OR DATE_PART('day', LOCALTIMESTAMP(0)-dln.dln_datetime)::integer >= @expiration_days",
                        expiration_days = expiration_days),
                        verbose = verbose,
                        render_sql = render_sql)



                        write_cg_staging_tbl(conn = conn,
                                             tableName = "temp_drug_link_table",
                                             data = drug_link_table,
                                             verbose = verbose,
                                             render_sql = render_sql)


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
                                        ",
                                    verbose = verbose,
                                    render_sql = render_sql
                        )

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

                        pg13::appendTable(conn = conn,
                                          schema = "cancergov",
                                          tableName = "DRUG_LINK_NCIT",
                                          data = results)


        }


#' @title
#' Process the Links found in the Drug Link Table for NCIt and other URLs
#'
#' @inherit cg_run description
#' @inheritSection cg_run Drug Detail Links
#' @inheritParams get_drug_link_synonym
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[secretary]{typewrite_progress}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#' @rdname process_drug_link_url
#' @export
#' @importFrom pg13 query appendTable
#' @importFrom SqlRender render
#' @importFrom secretary typewrite_progress typewrite magentaTxt
#' @importFrom rvest html_nodes html_attr
#' @importFrom tibble tibble

process_drug_link_url <-
        function(conn,
                 sleep_time = 5,
                 expiration_days = 30,
                 encoding = "",
                 options = c("RECOVER", "NOERROR", "NOBLANKS"),
                 verbose = TRUE,
                 render_sql = TRUE) {

                        drug_link_table <-
                                pg13::query(conn = conn,
                                            sql_statement =
                                                    SqlRender::render(
                                                        "
                                                        SELECT DISTINCT
                                                                dl.drug, dl.drug_link
                                                        FROM cancergov.drug_link dl
                                                        LEFT JOIN cancergov.drug_link_url dlu
                                                        ON dlu.drug_link  = dl.drug_link
                                                        WHERE dlu_datetime IS NULL
                                                                OR DATE_PART('day', LOCALTIMESTAMP(0)-dlu.dlu_datetime)::integer >= @expiration_days",
                                                        expiration_days = expiration_days))




                        drug_links <- unique(drug_link_table$drug_link)


                for (i in seq_along(drug_links)) {


                        # drug_link <- "https://www.cancer.gov/publications/dictionaries/cancer-drug/def/792667"
                        # drug_link <- "https://www.cancer.gov/publications/dictionaries/cancer-drug/def/61cu-atsm"

                        drug_link <- drug_links[i]

                        if (verbose) {

                                secretary::typewrite_progress(iteration = i,
                                                              total = length(drug_links))
                                secretary::typewrite(secretary::magentaTxt("Drug Link:"), drug_link)
                        }


                        response <- scrape(x = drug_link,
                                           encoding = encoding,
                                           options = options,
                                           verbose = verbose,
                                           sleep_time = sleep_time)




                        if (!is.null(response)) {

                                results <-
                                        tryCatch(
                                                response %>%
                                                        rvest::html_nodes(".navigation-dark-red") %>%
                                                        rvest::html_attr("href"),
                                                error = function(e) NULL
                                        )



                                if (length(results) > 0) {

                                        output  <-
                                                tibble::tibble(dlu_datetime = Sys.time(),
                                                               drug_link = drug_link,
                                                               drug_link_url = results)

                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "DRUG_LINK_URL",
                                                                  data = output)


                                } else {

                                        output  <-
                                                tibble::tibble(dlu_datetime = Sys.time(),
                                                               drug_link = drug_link,
                                                               drug_link_url = NA)

                                                pg13::appendTable(conn = conn,
                                                                  schema = "cancergov",
                                                                  tableName = "DRUG_LINK_URL",
                                                                  data = output)

                                }

                        }


                }

        }


#' @title
#' Scrape the NCI Thesaurus
#'
#' @description
#' All NCIt Codes that have not been scraped or were scraped in the expiration period are scraped in the NCIt Thesaurus at the \url{"https://ncithesaurus.nci.nih.gov/ncitbrowser/pages/concept_details.jsf?dictionary=NCI_Thesaurus&code=%s&ns=ncit&type=synonym&key=null&b=1&n=0&vse=null#} path.
#' @inheritSection cg_run Drug Detail Links
#' @inheritParams get_drug_link_synonym
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[secretary]{typewrite_progress}},\code{\link[secretary]{c("typewrite", "typewrite")}},\code{\link[secretary]{character(0)}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#'  \code{\link[purrr]{keep}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{select_all}}
#'  \code{\link[rubix]{format_colnames}}
#' @rdname get_ncit_synonym
#' @export
#' @importFrom pg13 query appendTable
#' @importFrom SqlRender render
#' @importFrom secretary typewrite_progress typewrite cyanTxt
#' @importFrom rvest html_nodes html_table
#' @importFrom purrr keep
#' @importFrom dplyr bind_rows mutate rename_all transmute
#' @importFrom rubix format_colnames

get_ncit_synonym <-
        function(conn,
                 sleep_time = 5,
                 expiration_days = 30,
                 verbose = TRUE,
                 render_sql = TRUE) {

                        ncit_synonym_table <-
                                pg13::query(conn = conn,
                                            sql_statement =
                                                    SqlRender::render("
                                        SELECT DISTINCT
                                                dln.*
                                        FROM cancergov.drug_link_ncit dln
                                        LEFT JOIN cancergov.ncit_synonym ns
                                        ON dln.ncit_code  = ns.ncit_code
                                        WHERE ns_datetime IS NULL
                                                OR DATE_PART('day', LOCALTIMESTAMP(0)-ns.ns_datetime)::integer >= @expiration_days",
                                            expiration_days = expiration_days),
                                            verbose = verbose,
                                            render_sql = render_sql
                                        )



                ncit_codes <- unique(ncit_synonym_table$ncit_code)


                for (i in 1:length(ncit_codes)) {

                        ncit_code <- ncit_codes[i]
                        ncit_code_url <- sprintf("https://ncithesaurus.nci.nih.gov/ncitbrowser/pages/concept_details.jsf?dictionary=NCI_Thesaurus&code=%s&ns=ncit&type=synonym&key=null&b=1&n=0&vse=null#", ncit_code)

                        if (verbose) {

                                secretary::typewrite_progress(iteration = i,
                                                              total = length(ncit_codes))
                                secretary::typewrite(secretary::cyanTxt("NCIt Code:"), ncit_code)
                        }

                        response <- scrape(ncit_code_url)


                        if (!is.null(response)) {

                                output <-
                                response %>%
                                        rvest::html_nodes("table") %>%
                                        rvest::html_table(fill = TRUE) %>%
                                        purrr::keep(function(x) "Term" %in% colnames(x)) %>%
                                        dplyr::bind_rows() %>%
                                        dplyr::mutate(ncit_code = ncit_code) %>%
                                        dplyr::mutate(ncit_code_url = ncit_code_url) %>%
                                        rubix::format_colnames() %>%
                                        dplyr::rename_all(tolower) %>%
                                        dplyr::transmute(ns_datetime = Sys.time(),
                                                         ncit_code,
                                                         ncit_code_url,
                                                         term,
                                                         source,
                                                         tty = type,
                                                         code)

                                pg13::appendTable(conn = conn,
                                                 schema = "cancergov",
                                                 tableName = "ncit_synonym",
                                                 data = output)

                        }

                }

        }


#' @title
#' Update the Cancergov Drugs Table
#'
#' @description
#' The Synonyms found in the NCI Drug Dictionary and NCI Thesaurus are aggregated into a single Cancergov Drug Table.
#'
#' @inheritParams cg_run
#'
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{send}}
#'  \code{\link[tidyr]{pivot_longer}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{coalesce}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{group_by}},\code{\link[dplyr]{arrange}},\code{\link[dplyr]{slice}},\code{\link[dplyr]{reexports}}
#'  \code{\link[forcats]{fct_recode}}
#' @rdname update_cancergov_drugs
#' @export
#' @importFrom pg13 query send
#' @importFrom tidyr pivot_longer
#' @importFrom dplyr mutate coalesce select distinct group_by arrange slice ungroup everything
#' @importFrom forcats fct_recode

update_cancergov_drugs <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE) {

                cancergov_drugs <-
                        pg13::query(
                                conn = conn,
                                sql_statement =
                                "
                                SELECT DISTINCT dl.drug_link, dln.ncit_code, dl.drug, dls.drug_synonym_type, dls.drug_synonym, ns.term as ncit_drug
                                FROM cancergov.drug_link dl
                                LEFT JOIN cancergov.drug_link_synonym dls
                                ON dls.drug_link = dl.drug_link
                                LEFT JOIN cancergov.drug_link_ncit dln
                                ON dln.drug_link = dl.drug_link
                                LEFT JOIN cancergov.ncit_synonym ns
                                ON dln.ncit_code = ns.ncit_code
                                ",
                                verbose = verbose,
                                render_sql = render_sql)


                cancergov_drugs2 <-
                        cancergov_drugs %>%
                        tidyr::pivot_longer(cols = c(drug, drug_synonym, ncit_drug),
                                            names_to = "drug_name_type",
                                            values_to = "cancergov_drug",
                                            values_drop_na = TRUE) %>%
                        dplyr::mutate(drug_name_type = factor(drug_name_type)) %>%
                        dplyr::mutate(drug_name_type = forcats::fct_recode(drug_name_type,
                                                                           `Label:` = "drug",
                                                                           `NCIt Term:` = "ncit_drug")) %>%
                        dplyr::mutate(drug_name_type = as.character(drug_name_type)) %>%
                        dplyr::mutate(drug_synonym_type = dplyr::coalesce(drug_synonym_type, drug_name_type)) %>%
                        dplyr::select(-drug_name_type) %>%
                        dplyr::distinct() %>%
                        dplyr::mutate(drug_synonym_type = factor(drug_synonym_type,
                                                                 levels = c("NCIt Term:",
                                                                            "Label:",
                                                                            "Synonym:",
                                                                            "Code name:",
                                                                            "Abbreviation:",
                                                                            "Acronym:",
                                                                            "US brand name:",
                                                                            "Chemical structure:",
                                                                            "Foreign brand name:"))) %>%
                        dplyr::group_by(drug_link, ncit_code, cancergov_drug) %>%
                        dplyr::arrange(desc(drug_synonym_type), .by_group = TRUE) %>%
                        dplyr::slice(1) %>%
                        dplyr::ungroup() %>%
                        dplyr::mutate(drug_synonym_type = as.character(drug_synonym_type)) %>%
                        dplyr::mutate(cd_datetime = Sys.time()) %>%
                        dplyr::select(cd_datetime,
                                      dplyr::everything())



                write_cg_staging_tbl(conn = conn,
                                     tableName = "chron_m01_cancergov_drugs2",
                                     data = cancergov_drugs2,
                                     verbose = verbose,
                                     render_sql = render_sql)


                pg13::send(conn = conn,
                           sql_statement =
                                   "
                                        WITH new_drugs AS (
                                        SELECT new.*
                                        FROM cancergov.chron_m01_cancergov_drugs2 new
                                        LEFT JOIN cancergov.cancergov_drug current
                                        ON current.drug_link = new.drug_link
                                                AND current.ncit_code = new.ncit_code
                                                AND current.drug_synonym_type = new.drug_synonym_type
                                                AND current.cancergov_drug = new.cancergov_drug
                                        WHERE current.cd_datetime IS NULL
                                        )

                                        INSERT INTO cancergov.cancergov_drug SELECT * FROM new_drugs
                                        ",
                           verbose = verbose,
                           render_sql = render_sql
                )

        }

