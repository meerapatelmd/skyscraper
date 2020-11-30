#' @title
#' Create PubMed Tables in Patelm9 Schema
#'
#' @description
#' Create PubMed Tables in the Patelm9 schema if they do not already exist. Tables include: PM_LOG, PM_EARLIEST, PM_LATEST, and PM_RESULTS*. PM_RESULTS is not created by this script. It is written by \code{\link{pm_run}}.
#'
#' @importFrom pg13 send
#' @export

start_pm <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE) {

                pg13::send(conn = conn,
                           sql_statement =
                                        "
                                        CREATE TABLE IF NOT EXISTS patelm9.pm_log (
                                                    rl_datetime timestamp without time zone,
                                                    search_type character varying(255),
                                                    search_term character varying(255),
                                                    processed_search_term character varying(255),
                                                    url character varying(255),
                                                    results_count integer
                                        );

                                        CREATE TABLE IF NOT EXISTS patelm9.pm_earliest (
                                                            r_datetime timestamp without time zone,
                                                            search_term character varying(255),
                                                            url text,
                                                            title text,
                                                            citation text,
                                                            snippet text,
                                                            citation_date_string character varying(25),
                                                            citation_date date
                                        );

                                        CREATE TABLE IF NOT EXISTS patelm9.pm_latest (
                                                            r_datetime timestamp without time zone,
                                                            search_term character varying(255),
                                                            url text,
                                                            title text,
                                                            citation text,
                                                            snippet text,
                                                            citation_date_string character varying(25),
                                                            citation_date date
                                        );",
                           verbose = verbose,
                           render_sql = render_sql)
        }
