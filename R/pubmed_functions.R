#' @title
#' Scrape PubMed Publications
#'
#' @description
#' A PubMed search is performed for a string given as the `search_term` argument and the earliest or the latest publications are returned in descending order. This is a useful tool for researching investigational drugs and whether or not they are a code name for an existing drug on the market. Note that there is no `expiration_days` argument because the earliest dates cannot ever become obsolete.
#'
#' @seealso
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#' @rdname get_pm
#' @family pubmed functions
#' @export
#' @importFrom stringr str_remove_all
#' @importFrom xml2 read_html
#' @importFrom rvest html_node html_text html_nodes
#' @importFrom tibble tibble
#' @importFrom pg13 lsSchema createSchema lsTables appendTable writeTable


get_pm <-
        function(conn,
                 conn_fun,
                 earliest = TRUE,
                 latest = !earliest,
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

                if (earliest) {

                        URL <- paste0("https://pubmed.ncbi.nlm.nih.gov/?term=", processed_search_term, "&filter=dates.1900%2F1%2F1-3000%2F12%2F12&sort=date&sort_order=asc")

                } else {

                        URL <- URL <- paste0("https://pubmed.ncbi.nlm.nih.gov/?term=", processed_search_term, "&sort=date")

                }

                if (earliest) {
                        search_type <- "earliest"
                } else {
                        search_type <- "latest"
                }

                current_results_log <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                            SqlRender::render("SELECT DISTINCT *
                                                                FROM patelm9.pm_log log
                                                                WHERE url IN ('@url')
                                                                        AND search_type = '@search_type';",
                                                              url = URL,
                                                              search_type = search_type),
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
                                                       search_type = search_type,
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
                                                       search_type = search_type,
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


                        if (earliest) {
                                pg13::appendTable(conn = conn,
                                                  schema = "patelm9",
                                                  tableName = "PM_EARLIEST",
                                                  data = results)
                        } else {
                                pg13::appendTable(conn = conn,
                                                  schema = "patelm9",
                                                  tableName = "PM_LATEST",
                                                  data = results)
                        }
                }
        }


#' @title
#' Get Earliest PubMed Publications
#'
#' @description
#' Get the earliest PubMed publications for a given term. The results are stored in the PM_EARLIEST Table. See \code{\link{get_pm}} for more details.
#'
#' @inheritParams get_pm
#' @rdname get_pm_earliest
#' @family pubmed functions
#' @export

get_pm_earliest <-
        function(conn,
                 conn_fun,
                 search_term,
                 verbose = TRUE,
                 render_sql = TRUE) {

                get_pm(conn = conn,
                       conn_fun = conn_fun,
                       earliest = TRUE,
                       search_term = search_term,
                       verbose = verbose,
                       render_sql = render_sql)
        }

#' @title
#' Get Latest PubMed Publications
#'
#' @description
#' Get the latest PubMed publications for a given term. The results are stored in the PM_LATEST Table. See \code{\link{get_pm}} for more details.
#'
#' @inheritParams get_pm
#' @rdname get_pm_latest
#' @family pubmed functions
#' @export

get_pm_latest <-
        function(conn,
                 conn_fun,
                 search_term,
                 verbose = TRUE,
                 render_sql = TRUE) {

                get_pm(conn = conn,
                       conn_fun = conn_fun,
                       earliest = FALSE,
                       latest = TRUE,
                       search_term = search_term,
                       verbose = verbose,
                       render_sql = render_sql)
        }

#' @title
#' Run the complete PubMed Scrape
#'
#' @description
#' Scrape and the store the earliest and latest PubMed Publications for a search term by executing \code{\link{get_pm_earliest}} in concert with \code{\link{get_pm_latest}}. This is followed by an indiscriminate union between PM_EARLIEST and PM_LATEST Tables and refreshed as the PM_RESULTS Table.
#'
#' @export
#' @importFrom pg13 send
#' @rdname pm_run

pm_run <-
        function(conn,
                 conn_fun,
                 search_term,
                 verbose = TRUE,
                 render_sql = TRUE) {

                get_pm_earliest(conn = conn,
                                conn_fun = conn_fun,
                                search_term = search_term,
                                verbose = verbose,
                                render_sql = render_sql)

                get_pm_latest(
                        conn = conn,
                        conn_fun = conn_fun,
                        search_term = search_term,
                        verbose = verbose,
                        render_sql = render_sql)


                pg13::send(
                        conn = conn,
                        sql_statement =
                                "DROP TABLE IF EXISTS patelm9.pm_results;
                                CREATE TABLE IF NOT EXISTS patelm9.pm_results (
                                                    r_datetime timestamp without time zone,
                                                            search_term character varying(255),
                                                            url text,
                                                            title text,
                                                            citation text,
                                                            snippet text,
                                                            citation_date_string character varying(25),
                                                            citation_date date,
                                                            search_type text
                                        );

                                WITH both_tables AS (
                                        SELECT e.*, 'earliest' AS search_type
                                        FROM patelm9.pm_earliest e
                                        UNION
                                         SELECT l.*, 'latest' AS search_type
                                        FROM patelm9.pm_latest l
                                )

                                INSERT INTO patelm9.pm_results SELECT * FROM both_tables
                                ;
                        ",
                        verbose = verbose,
                        render_sql = render_sql
                )
        }





