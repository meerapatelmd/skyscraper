#' @title
#' Scrape a PubMed search
#' @description
#' The search is constrained to return results in the order of descending publication date.
#' @seealso
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#' @rdname scrape_pubmed
#' @family pubmed
#' @export
#' @importFrom stringr str_remove_all
#' @importFrom xml2 read_html
#' @importFrom rvest html_node html_text html_nodes
#' @importFrom tibble tibble
#' @importFrom pg13 lsSchema createSchema lsTables appendTable writeTable

scrape_recent_pubmed <-
        function(conn,
                 search_term,
                 schema = "patelm9") {


                #search_term <- "TP-1287"

                # if (pg13::isClosed(conn)) {
                #         conn <- chariot::connectAthena()
                # }

                processed_search_term <- stringr::str_remove_all(search_term, pattern = " ")
                URL <- paste0("https://pubmed.ncbi.nlm.nih.gov/?term=", processed_search_term, "&sort=date")

                if (!missing(conn)) {

                        schemas <- pg13::lsSchema(conn)

                        if (!(schema %in% schemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = schema)

                        }

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = schema)


                        if ("PUBMED_RESULTS_LOG" %in% Tables) {

                                current_results_log <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(schema = schema,
                                                                                     tableName = "PUBMED_RESULTS_LOG",
                                                                                     whereInField = "url",
                                                                                     whereInVector = URL,
                                                                                     distinct = TRUE))

                        }
                }



                if (!missing(conn)) {

                        if ("PUBMED_RESULTS_LOG" %in% Tables) {

                                proceed <- nrow(current_results_log) == 0

                        } else {

                                proceed <- TRUE

                        }

                } else {

                        proceed <- TRUE

                }





                if (proceed) {

                Sys.sleep(5)
                pubmed_scrape <- xml2::read_html(URL)

                results_count <-
                        pubmed_scrape %>%
                        rvest::html_node(".results-amount") %>%
                        rvest::html_text() %>%
                        trimws(which = "both")


                if (results_count %in% "No results were found.") {
                        results_log <-
                                tibble::tibble(rl_datetime = Sys.time(),
                                               search_term = search_term,
                                               processed_search_term = processed_search_term,
                                               url = URL,
                                               max_return_size = NA,
                                               results_count = 0)

                        results <-
                                tibble::tibble(
                                        r_datetime = Sys.time(),
                                        url = URL,
                                        title = NA,
                                        citation = NA,
                                        snippet = NA)

                } else {

                        results_count <-
                        results_count %>%
                        stringr::str_remove_all(pattern = "[^0-9]") %>%
                        as.integer()


                        results_log <-
                                tibble::tibble(rl_datetime = Sys.time(),
                                               search_term = search_term,
                                               processed_search_term = processed_search_term,
                                               url = URL,
                                               max_return_size = NA,
                                               results_count = results_count)


                        pub_snippet <-
                                pubmed_scrape %>%
                                rvest::html_nodes(".full-view-snippet") %>%
                                rvest::html_text(trim = TRUE)

                        pub_citation <-
                                pubmed_scrape %>%
                                rvest::html_nodes(".full-journal-citation") %>%
                                rvest::html_text(trim = TRUE)

                        pub_title <-
                                pubmed_scrape %>%
                                rvest::html_nodes(".docsum-title") %>%
                                rvest::html_text(trim = TRUE)

                        results <<-
                        tibble::tibble(
                                      r_datetime = Sys.time(),
                                      url = URL,
                                      title = pub_title,
                                      citation = pub_citation,
                                      snippet = pub_snippet)  %>%
                                tidyr::extract(col = citation,
                                               into = "citation_date_string",
                                               regex = ".*?([1-2]{1}[0-9]{3}[ ]{1,}[A-Za-z]{3,}).*$") %>%
                                dplyr::filter(!is.na(citation_date_string)) %>%
                                dplyr::mutate(citation_date = as.Date(lubridate::parse_date_time(citation_date_string, orders = "%Y %b"))) %>%
                                dplyr::select(r_datetime,
                                              url,
                                              title,
                                              snippet,
                                              citation_date_string,
                                              citation_date)

                }


                if (!missing(conn)) {

                        schemas <- pg13::lsSchema(conn)

                        if (!(schema %in% schemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = schema)

                        }

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = schema)


                        if ("PUBMED_RESULTS_LOG" %in% Tables) {

                                pg13::appendTable(conn = conn,
                                                  schema = schema,
                                                  tableName = "PUBMED_RESULTS_LOG",
                                                  results_log)
                        } else {
                                pg13::writeTable(conn = conn,
                                                 schema =schema,
                                                 tableName = "PUBMED_RESULTS_LOG",
                                                 results_log)
                        }

                }

                if (!missing(conn)) {

                        schemas <- pg13::lsSchema(conn)

                        if (!(schema %in% schemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = schema)

                        }

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = schema)


                        if (!("PUBMED_RESULTS" %in% Tables)) {

                                pg13::send(conn = conn,
                                           sql_statement =
                                                   SqlRender::render(
                                                "CREATE TABLE @schema.results (

                                                r_datetime timestamp without time zone,
                                                url character varying(255),
                                                title TEXT,
                                                citation TEXT,
                                                snippet TEXT,
                                                citation_date_string VARCHAR(25),
                                                citation_date_string DATE
                                                );
                                                ",
                                                schema = schema))


                                pg13::appendTable(conn = conn,
                                                  schema = schema,
                                                  tableName = "PUBMED_RESULTS",
                                                  results)

                        } else {

                                pg13::appendTable(conn = conn,
                                                  schema = schema,
                                                  tableName = "PUBMED_RESULTS",
                                                  results)


                        }

                }

                if (missing(conn)) {

                        output <- list(RESULTS_LOG = results_log,
                                       RESULTS = results)

                        output

                        }
                }

        }


#' @title
#' Scrape a PubMed search
#' @description
#' The search is constrained to return results in the order of descending publication date.
#' @seealso
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#' @rdname scrape_pubmed
#' @family pubmed
#' @export
#' @importFrom stringr str_remove_all
#' @importFrom xml2 read_html
#' @importFrom rvest html_node html_text html_nodes
#' @importFrom tibble tibble
#' @importFrom pg13 lsSchema createSchema lsTables appendTable writeTable

scrape_earliest_pubmed <-
        function(conn,
                 search_term,
                 schema = "patelm9") {


                #search_term <- "TP-1287"

                # if (pg13::isClosed(conn)) {
                #         conn <- chariot::connectAthena()
                # }

                processed_search_term <- stringr::str_remove_all(search_term, pattern = " ")
                URL <- paste0("https://pubmed.ncbi.nlm.nih.gov/?term=", processed_search_term, "&filter=dates.1900%2F1%2F1-3000%2F12%2F12&sort=date&sort_order=asc")


                if (!missing(conn)) {

                        schemas <- pg13::lsSchema(conn)

                        if (!(schema %in% schemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = schema)

                        }

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = schema)


                        if ("PUBMED_EARLIEST_RESULTS_LOG" %in% Tables) {

                                current_results_log <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(schema = schema,
                                                                                     tableName = "PUBMED_EARLIEST_RESULTS_LOG",
                                                                                     whereInField = "url",
                                                                                     whereInVector = URL,
                                                                                     distinct = TRUE))

                        }
                }



                if (!missing(conn)) {

                        if ("PUBMED_EARLIEST_RESULTS_LOG" %in% Tables) {

                                proceed <- nrow(current_results_log) == 0

                        } else {

                                proceed <- TRUE

                        }

                } else {

                        proceed <- TRUE

                }





                if (proceed) {

                        Sys.sleep(5)
                        pubmed_scrape <- xml2::read_html(URL)

                        results_count <-
                                pubmed_scrape %>%
                                rvest::html_node(".results-amount") %>%
                                rvest::html_text() %>%
                                trimws(which = "both")


                        if (results_count %in% "No results were found.") {
                                results_log <-
                                        tibble::tibble(rl_datetime = Sys.time(),
                                                       search_term = search_term,
                                                       processed_search_term = processed_search_term,
                                                       url = URL,
                                                       max_return_size = NA,
                                                       results_count = 0)

                                results <-
                                        tibble::tibble(
                                                r_datetime = Sys.time(),
                                                url = URL,
                                                title = NA,
                                                citation = NA,
                                                snippet = NA)

                        } else {

                                results_count <-
                                        results_count %>%
                                        stringr::str_remove_all(pattern = "[^0-9]") %>%
                                        as.integer()


                                results_log <-
                                        tibble::tibble(rl_datetime = Sys.time(),
                                                       search_term = search_term,
                                                       processed_search_term = processed_search_term,
                                                       url = URL,
                                                       max_return_size = NA,
                                                       results_count = results_count)


                                pub_snippet <-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".full-view-snippet") %>%
                                        rvest::html_text(trim = TRUE)

                                pub_citation <-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".full-journal-citation") %>%
                                        rvest::html_text(trim = TRUE)

                                pub_title <-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".docsum-title") %>%
                                        rvest::html_text(trim = TRUE)

                                results <-
                                        tibble::tibble(
                                                r_datetime = Sys.time(),
                                                url = URL,
                                                title = pub_title,
                                                citation = pub_citation,
                                                snippet = pub_snippet)  %>%
                                        tidyr::extract(col = citation,
                                                       into = "citation_date_string",
                                                       regex = ".*?([1-2]{1}[0-9]{3}[ ]{1,}[A-Za-z]{3,}).*$") %>%
                                        dplyr::filter(!is.na(citation_date_string)) %>%
                                        dplyr::mutate(citation_date = as.Date(lubridate::parse_date_time(citation_date_string, orders = "%Y %b"))) %>%
                                        dplyr::select(r_datetime,
                                                      url,
                                                      title,
                                                      snippet,
                                                      citation_date_string,
                                                      citation_date)

                        }


                        if (!missing(conn)) {

                                schemas <- pg13::lsSchema(conn)

                                if (!(schema %in% schemas)) {

                                        pg13::createSchema(conn = conn,
                                                           schema = schema)

                                }

                                Tables <- pg13::lsTables(conn = conn,
                                                         schema = schema)


                                if ("PUBMED_EARLIEST_RESULTS_LOG" %in% Tables) {

                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "PUBMED_EARLIEST_RESULTS_LOG",
                                                          results_log)
                                } else {
                                        pg13::writeTable(conn = conn,
                                                         schema =schema,
                                                         tableName = "PUBMED_EARLIEST_RESULTS_LOG",
                                                         results_log)
                                }

                        }

                        if (!missing(conn)) {

                                schemas <- pg13::lsSchema(conn)

                                if (!(schema %in% schemas)) {

                                        pg13::createSchema(conn = conn,
                                                           schema = schema)

                                }

                                Tables <- pg13::lsTables(conn = conn,
                                                         schema = schema)


                                if (!("PUBMED_EARLIEST_RESULTS" %in% Tables)) {

                                        pg13::send(conn = conn,
                                                   sql_statement =
                                                           SqlRender::render(
                                                                   "CREATE TABLE @schema.results (

                                                r_datetime timestamp without time zone,
                                                url character varying(255),
                                                title TEXT,
                                                citation TEXT,
                                                snippet TEXT,
                                                citation_date_string VARCHAR(25),
                                                citation_date DATE
                                                );
                                                ",
                                                                   schema = schema))


                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "PUBMED_EARLIEST_RESULTS",
                                                          results)

                                } else {

                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "PUBMED_EARLIEST_RESULTS",
                                                          results)


                                }

                        }

                        if (missing(conn)) {

                                output <- list(RESULTS_LOG = results_log,
                                               RESULTS = results)

                                output

                        }
                }

        }

