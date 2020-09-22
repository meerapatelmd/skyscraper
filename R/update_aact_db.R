#' @title
#' Update ClinicalTrial.gov `aact` Postgres Database
#'
#' @inherit clinicaltrialsgov_functions description
#' @inheritSection clinicaltrialsgov_functions AACT Database
#'
#' @return
#' A `aact` Postgres Database if one did not previously exist with a populated `ctgov` schema and a log of the source file and timestamp in the `public.update_log` Table.
#'
#' @seealso
#'  \code{\link[police]{try_catch_error_as_null}}
#'  \code{\link[httr]{with_config}},\code{\link[httr]{c("add_headers", "authenticate", "config", "config", "set_cookies", "timeout", "use_proxy", "user_agent", "verbose")}},\code{\link[httr]{GET}},\code{\link[httr]{content}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[curl]{handle}},\code{\link[curl]{curl_download}}
#'  \code{\link[pg13]{query}},\code{\link[pg13]{createDB}},\code{\link[pg13]{localConnect}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}},\code{\link[pg13]{dc}}
#' @rdname update_aact_db
#' @export
#' @importFrom police try_catch_error_as_null
#' @importFrom httr with_config config GET content
#' @importFrom rvest html_nodes html_children html_text
#' @importFrom curl new_handle curl_download
#' @importFrom pg13 query createDB localConnect lsTables appendTable writeTable dc
#' @importFrom magrittr %>%


update_aact_db <-
        function(conn) {


                # Reading ACCT Page
                aact_page <-
                        police::try_catch_error_as_null(
                                httr::with_config(
                                        config = httr::config(ssl_verifypeer = FALSE),
                                        httr::GET("https://aact.ctti-clinicaltrials.org/snapshots")
                                ) %>%
                                        httr::content()
                        )



                # aact_page <- xml2::read_html("https://aact.ctti-clinicaltrials.org/snapshots")


                # Parse Most Recent Filename
                file_archive <-
                        aact_page %>%
                        rvest::html_nodes(".file-archive td") %>%
                        rvest::html_children() %>%
                        rvest::html_text()
                file_archive <- file_archive[1]


                # Download most recent filename
                handle <- curl::new_handle(ssl_verifypeer=FALSE)
                curl::curl_download(
                        paste0("https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/", file_archive),
                        destfile = file_archive,
                        quiet = FALSE,
                        handle = handle)

                Sys.sleep(0.2)

                # Unzip
                unzip(file_archive)


                # If an "aact" database does not exist, create one
                DB <-
                pg13::query(conn = conn,
                            sql_statement =
                                            "
                                            SELECT datname
                                            FROM pg_database
                                            WHERE datistemplate = false;
                                            "
                            ) %>%
                        unlist()

                if ("aact" %in% DB) {

                        # Run run postgres_data.dmp from download dir
                        system(command = paste0("pg_restore -e -v -O -x -d aact --clean --no-owner ", getwd(), "/postgres_data.dmp"))
                } else {
                        pg13::createDB(conn = conn,
                                       newDB = "aact")

                        system(command = paste0("pg_restore -e -v -O -x -d aact --no-owner ", getwd(), "/postgres_data.dmp"))

                }

                aact_conn <- pg13::localConnect(dbname = "aact")
                Tables <- pg13::lsTables(conn = aact_conn,
                                        schema = "public")
                if ("UPDATE_LOG" %in% Tables) {
                        pg13::appendTable(conn = aact_conn,
                                          schema = "public",
                                          tableName = "UPDATE_LOG",
                                          data.frame(ul_datetime = Sys.time(),
                                                     filename = file_archive))
                } else {
                        pg13::writeTable(conn = aact_conn,
                                         schema = "public",
                                         tableName = "UPDATE_LOG",
                                         data.frame(ul_datetime = Sys.time(),
                                                    filename = file_archive))
                }
                pg13::dc(aact_conn, remove = TRUE)

                # Remove all files
                file.remove("schema_diagram.png",
                            "admin_schema_diagram.png",
                            "nlm_results_definitions.html",
                            "nlm_protocol_definitions.html",
                            "postgres_data.dmp",
                            "data_dictionary.xlsx",
                            file_archive)
        }
