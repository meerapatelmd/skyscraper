#' @title
#' Scrape Earliest PubMed Publications
#'
#' @description
#' A PubMed search is performed for a string given as the `search_term` argument and the earliest publications are returned in descending order. This is a useful tool for researching investigational drugs and whether or not they are a code name for an existing drug on the market. Note that there is no `expiration_days` argument because the earliest dates cannot ever become obsolete.
#'
#' @seealso
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#' @rdname scrape_earliest_pm
#' @family pubmed
#' @export
#' @importFrom stringr str_remove_all
#' @importFrom xml2 read_html
#' @importFrom rvest html_node html_text html_nodes
#' @importFrom tibble tibble
#' @importFrom pg13 lsSchema createSchema lsTables appendTable writeTable


get_pm_earliest <-
        function(conn,
                 conn_fun,
                 search_term,
                 verbose = TRUE,
                 render_sql = TRUE) {

                if (!missing(conn_fun)) {

                        conn <- eval(rlang::parse_expr(conn_fun))
                        on.exit(pg13::dc(conn = conn,
                                         verbose = verbose))

                }


                # search_term <- "Meera"
                # verbose <- TRUE
                # render_sql <- TRUE

                start_pm(conn = conn,
                         verbose = verbose,
                         render_sql = render_sql)



                processed_search_term <- urltools::url_encode(search_term)
                URL <- paste0("https://pubmed.ncbi.nlm.nih.gov/?term=", processed_search_term, "&filter=dates.1900%2F1%2F1-3000%2F12%2F12&sort=date&sort_order=asc")


                current_results_log <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                            SqlRender::render("SELECT DISTINCT *
                                                                FROM patelm9.pm_log log
                                                                WHERE url IN ('@url')
                                                                        AND search_type = 'earliest';",
                                                              url = URL),
                                    verbose = verbose,
                                    render_sql = render_sql)

                proceed <- nrow(current_results_log) == 0


                if (proceed) {


                        pubmed_scrape <- scrape(URL)

                        results_count <-
                                pubmed_scrape %>%
                                rvest::html_node(".results-amount") %>%
                                rvest::html_text() %>%
                                trimws(which = "both")


                        if (results_count %in% "No results were found.") {

                                results_log <-
                                        tibble::tibble(rl_datetime = Sys.time(),
                                                       search_type = "earliest",
                                                       search_term = search_term,
                                                       processed_search_term = processed_search_term,
                                                       url = URL,
                                                       results_count = 0)

                                results <-
                                        tibble::tibble(
                                                r_datetime = Sys.time(),
                                                search_term = search_term,
                                                url = URL,
                                                title = NA,
                                                citation = NA,
                                                snippet = NA,
                                                citation_date_string = NA,
                                                citation_date = NA)

                        } else {

                                rl_datetime <- Sys.time()

                                results_count <-
                                        results_count %>%
                                        stringr::str_remove_all(pattern = "[^0-9]") %>%
                                        as.integer()


                                results_log <-
                                        tibble::tibble(rl_datetime = rl_datetime,
                                                       search_type = "earliest",
                                                       search_term = search_term,
                                                       processed_search_term = processed_search_term,
                                                       url = URL,
                                                       results_count = results_count)


                                pub_snippet <-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".full-view-snippet") %>%
                                        rvest::html_text(trim = TRUE)

                                if (length(pub_snippet) == 0) {
                                        pub_snippet <- NA
                                }

                                pub_citation <<-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".full-journal-citation") %>%
                                        rvest::html_text(trim = TRUE)

                                pub_title <<-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".docsum-title") %>%
                                        rvest::html_text(trim = TRUE)

                                results <-
                                        tibble::tibble(
                                                r_datetime = rl_datetime,
                                                search_term = search_term,
                                                url = URL,
                                                title = pub_title,
                                                citation = pub_citation,
                                                snippet = pub_snippet)  %>%
                                        tidyr::extract(col = citation,
                                                       into = "citation_date_string",
                                                       regex = ".*?([1-2]{1}[0-9]{3}[ ]{1,}[A-Za-z]{3,}).*$",
                                                       remove = FALSE) %>%
                                        dplyr::filter(!is.na(citation_date_string)) %>%
                                        dplyr::mutate(citation_date = as.Date(lubridate::parse_date_time(citation_date_string, orders = "%Y %b"))) %>%
                                        dplyr::select(r_datetime,
                                                      search_term,
                                                      url,
                                                      title,
                                                      snippet,
                                                      citation,
                                                      citation_date_string,
                                                      citation_date)

                        }


                        pg13::appendTable(conn = conn,
                                          schema = "patelm9",
                                          tableName = "PM_LOG",
                                          data = results_log)


                        pg13::appendTable(conn = conn,
                                          schema = "patelm9",
                                          tableName = "PM_EARLIEST",
                                          data = results)
                }
        }

#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @param search_term PARAM_DESCRIPTION
#' @param schema PARAM_DESCRIPTION, Default: 'patelm9'
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}},\code{\link[pg13]{send}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[tidyr]{extract}}
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{select}}
#'  \code{\link[lubridate]{parse_date_time}}
#' @rdname scrape_latest_pubmed
#' @export
#' @importFrom stringr str_remove_all
#' @importFrom pg13 lsSchema createSchema lsTables query appendTable writeTable send
#' @importFrom SqlRender render
#' @importFrom xml2 read_html
#' @importFrom rvest html_node html_text html_nodes
#' @importFrom tibble tibble
#' @importFrom tidyr extract
#' @importFrom dplyr filter mutate select
#' @importFrom lubridate parse_date_time


scrape_latest_pubmed <-
        function(conn,
                 search_term,
                 schema = "patelm9") {

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


                        if ("PUBMED_SEARCH_LOG" %in% Tables) {

                                current_results_log <-
                                        pg13::query(conn = conn,
                                                    sql_statement =
                                                            SqlRender::render("
                                                                              SELECT DISTINCT *
                                                                              FROM @schema.pubmed_search_log log
                                                                              WHERE url IN ('@url')
                                                                                AND search_type = 'latest'
                                                                                ;
                                                                              ",
                                                                              schema = schema,
                                                                              url = URL)
                                        )

                        }
                }



                if (!missing(conn)) {

                        if ("PUBMED_SEARCH_LOG" %in% Tables) {

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
                                                       search_type = "latest",
                                                       search_term = search_term,
                                                       processed_search_term = processed_search_term,
                                                       url = URL,
                                                       results_count = 0)

                                results <-
                                        tibble::tibble(
                                                r_datetime = Sys.time(),
                                                search_term = search_term,
                                                url = URL,
                                                title = NA,
                                                citation = NA,
                                                snippet = NA,
                                                citation_date_string = NA,
                                                citation_date = NA)

                        } else {

                                results_count <-
                                        results_count %>%
                                        stringr::str_remove_all(pattern = "[^0-9]") %>%
                                        as.integer()


                                results_log <-
                                        tibble::tibble(rl_datetime = Sys.time(),
                                                       search_type = "latest",
                                                       search_term = search_term,
                                                       processed_search_term = processed_search_term,
                                                       url = URL,
                                                       results_count = results_count)


                                pub_snippet <-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".full-view-snippet") %>%
                                        rvest::html_text(trim = TRUE)

                                if (length(pub_snippet) == 0) {
                                        pub_snippet <- NA
                                }

                                pub_citation <<-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".full-journal-citation") %>%
                                        rvest::html_text(trim = TRUE)

                                pub_title <<-
                                        pubmed_scrape %>%
                                        rvest::html_nodes(".docsum-title") %>%
                                        rvest::html_text(trim = TRUE)

                                results <<-
                                        tibble::tibble(
                                                r_datetime = Sys.time(),
                                                search_term = search_term,
                                                url = URL,
                                                title = pub_title,
                                                citation = pub_citation,
                                                snippet = pub_snippet)  %>%
                                        tidyr::extract(col = citation,
                                                       into = "citation_date_string",
                                                       regex = ".*?([1-2]{1}[0-9]{3}[ ]{1,}[A-Za-z]{3,}).*$",
                                                       remove = FALSE) %>%
                                        dplyr::filter(!is.na(citation_date_string)) %>%
                                        dplyr::mutate(citation_date = as.Date(lubridate::parse_date_time(citation_date_string, orders = "%Y %b"))) %>%
                                        dplyr::select(r_datetime,
                                                      search_term,
                                                      url,
                                                      title,
                                                      snippet,
                                                      citation,
                                                      citation_date_string,
                                                      citation_date)

                        }


                        if (!missing(conn)) {

                                Tables <- pg13::lsTables(conn = conn,
                                                         schema = schema)

                                if ("PUBMED_SEARCH_LOG" %in% Tables) {

                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "PUBMED_SEARCH_LOG",
                                                          results_log)
                                } else {
                                        pg13::writeTable(conn = conn,
                                                         schema =schema,
                                                         tableName = "PUBMED_SEARCH_LOG",
                                                         results_log)
                                }



                                if (!("PUBMED_LATEST_RESULTS" %in% Tables)) {

                                        pg13::send(conn = conn,
                                                   sql_statement =
                                                           SqlRender::render(
                                                                   "CREATE TABLE @schema.PUBMED_LATEST_RESULTS (
                                                                        r_datetime timestamp without time zone,
                                                                        search_term character varying(255),
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
                                                          tableName = "PUBMED_LATEST_RESULTS",
                                                          results)

                                } else {

                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "PUBMED_LATEST_RESULTS",
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


