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

                start_pm(conn = conn,
                         verbose = verbose,
                         render_sql = render_sql)

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
