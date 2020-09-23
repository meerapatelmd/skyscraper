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

scrape_pubmed <-
        function(conn,
                 search_term,
                 max_return_size = 5) {


                # search_term <- "PONATINIB"

                # if (pg13::isClosed(conn)) {
                #         conn <- chariot::connectAthena()
                # }

                processed_search_term <- stringr::str_remove_all(search_term, pattern = " ")
                URL <- paste0("https://pubmed.ncbi.nlm.nih.gov/?term=", processed_search_term, "&sort=date&size=", max_return_size)

                if (!missing(conn)) {

                        schemas <- pg13::lsSchema(conn)

                        if (!("pubmed_search" %in% schemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = "pubmed_search")

                        }

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = "pubmed_search")


                        if ("RESULTS_LOG" %in% Tables) {

                                current_results_log <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(schema = "pubmed_search",
                                                                                     tableName = "results_log",
                                                                                     whereInField = "url",
                                                                                     whereInVector = URL,
                                                                                     distinct = TRUE))

                        }
                }



                if (!missing(conn)) {

                        if ("RESULTS_LOG" %in% Tables) {

                                proceed <- nrow(current_results_log) == 0

                        } else {

                                proceed <- TRUE

                        }

                } else {

                        proceed <- TRUE

                }





                if (proceed) {


                pubmed_scrape <- xml2::read_html(URL)

                results_count <-
                        pubmed_scrape %>%
                        rvest::html_node(".results-amount") %>%
                        rvest::html_text() %>%
                        stringr::str_remove_all(pattern = "[^0-9]") %>%
                        as.integer()


                results_log <-
                        tibble::tibble(rl_datetime = Sys.time(),
                                       search_term = search_term,
                                       processed_search_term = processed_search_term,
                                       url = URL,
                                       max_return_size = max_return_size,
                                       results_count = results_count)


                if (!missing(conn)) {

                        schemas <- pg13::lsSchema(conn)

                        if (!("pubmed_search" %in% schemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = "pubmed_search")

                        }

                        Tables <- pg13::lsTables(conn = conn,
                                       schema = "pubmed_search")


                        if ("RESULTS_LOG" %in% Tables) {

                                pg13::appendTable(conn = conn,
                                                  schema ="pubmed_search",
                                                  tableName = "results_log",
                                                  results_log)
                        } else {
                                pg13::writeTable(conn = conn,
                                                 schema ="pubmed_search",
                                                 tableName = "results_log",
                                                 results_log)
                        }

                }


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
                              snippet = pub_snippet)


                if (!missing(conn)) {

                        schemas <- pg13::lsSchema(conn)

                        if (!("pubmed_search" %in% schemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = "pubmed_search")

                        }

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = "pubmed_search")


                        if (!("RESULTS" %in% Tables)) {

                                pg13::send(conn = conn,
                                           sql_statement =

                                           "CREATE TABLE pubmed_search.results (

                                                r_datetime timestamp without time zone,
                                                url character varying(255),
                                                title TEXT,
                                                citation TEXT,
                                                snippet TEXT

                                                );
                                                ")


                                pg13::appendTable(conn = conn,
                                                  schema ="pubmed_search",
                                                  tableName = "results",
                                                  results)

                        } else {

                                pg13::appendTable(conn = conn,
                                                  schema ="pubmed_search",
                                                  tableName = "results",
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



