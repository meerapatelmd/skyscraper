#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details https://evs.nci.nih.gov/evs-download/thesaurus-downloads
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[pg13]{send}},\code{\link[pg13]{dropTable}}
#'  \code{\link[SqlRender]{render}}
#' @rdname load_mrconso
#' @family nci evs schema
#' @export
#' @importFrom pg13 send dropTable
#' @importFrom SqlRender render


load_mrconso <-
        function(umls_conn,
                 conn) {

                # umls_conn <- metaorite::connectLocalMT()
                # conn <- chariot::connectAthena()

                secretary::typewrite_italic("Warning: this process requires approx 1 hour and will drop the existing MRCONSO Table")
                secretary::press_enter()

                schema <- "nci_evs"
                tableName <- "mrconso"


                secretary::typewrite_italic(secretary::timepunch(),"\tReading MRCONSO Table from UMLS MTH instance....")

                mrconso <-
                        preQL::query(
                                conn = umls_conn,
                                sql_statement = "SELECT * FROM MRCONSO WHERE LAT = 'ENG';"
                        )

                mrconso2 <-
                        mrconso %>%
                        rubix::rm_multibyte_chars()

                Sys.sleep(1)

                secretary::typewrite_italic(secretary::timepunch(),"\tWriting MRCONSO Table to", temp, "...")

                temp <- tempfile(fileext = ".csv")
                # temp <- path.expand("~/Desktop/test_mrconso.csv")
                readr::write_csv(mrconso2,
                                 temp)


                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tCreating", schema, "if it didn't exist...")
                pg13::send(conn = conn,
                           sql_statement =
                                   SqlRender::render(
                                           "CREATE SCHEMA IF NOT EXISTS @schema;",
                                           schema = schema))

                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tMRCONSO Table in", schema, "dropped if it existed...")
                pg13::dropTable(conn = conn,
                                schema = schema,
                                tableName = tableName)

                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tDDLing MRCONSO Table...")

                pg13::send(conn = conn,
                           sql_statement =
                                                SqlRender::render(
                                                "CREATE TABLE @schema.@tableName (
                                                    cui character(10) NOT NULL,
                                                    lat character(3) NOT NULL,
                                                    ts character(1) NOT NULL,
                                                    lui character(10) NOT NULL,
                                                    stt character varying(3) NOT NULL,
                                                    sui character(10) NOT NULL,
                                                    ispref character(1) NOT NULL,
                                                    aui character varying(10) NOT NULL,
                                                    saui text,
                                                    scui text,
                                                    sdui text,
                                                    sab character varying(20) NOT NULL,
                                                    tty character varying(20) NOT NULL,
                                                    code text,
                                                    str text NOT NULL,
                                                    srl text NOT NULL,
                                                    suppress character(1) NOT NULL,
                                                    cvf integer,
                                                    filler_column text
                                                )",
                                                schema = schema,
                                                tableName = tableName
                                                )
                           )


                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tCopying", temp, "to MRCONSO Table in", schema, "...")

                pg13::send(conn = conn,
                                SqlRender::render(
                                SqlRender::readSql(system.file(package = "skyscraper", "sql/copy_from_csv.sql")),
                                tableName = tableName,
                                schema = schema,
                                vocabulary_file = temp))

                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tWriting indexes...")

                pg13::send(conn = conn,
                           sql_statement =
                                   SqlRender::render(
                                        "CREATE INDEX X_MRCONSO_CUI ON @schema.MRCONSO(CUI);
                                        ALTER TABLE @schema.MRCONSO ADD CONSTRAINT X_MRCONSO_PK PRIMARY KEY (AUI);
                                        CREATE INDEX X_MRCONSO_SUI ON @schema.MRCONSO(SUI);
                                        CREATE INDEX X_MRCONSO_LUI ON @schema.MRCONSO(LUI);
                                        CREATE INDEX X_MRCONSO_CODE ON @schema.MRCONSO(CODE);
                                        CREATE INDEX X_MRCONSO_SAB_TTY ON @schema.MRCONSO(SAB,TTY);
                                        CREATE INDEX X_MRCONSO_SCUI ON @schema.MRCONSO(SCUI);
                                        CREATE INDEX X_MRCONSO_STR ON @schema.MRCONSO(STR);",
                                        schema = schema))

                unlink(temp)

                load_log <-
                        data.frame(ll_datetime = Sys.time(),
                                   ll_table =  tableName)


                Tables <- lsTables(conn = conn,
                                   schema = schema)

                if ("LOAD_LOG" %in% Tables) {

                        pg13::appendTable(conn = conn,
                                          schema = schema,
                                          tableName = "LOAD_LOG",
                                          load_log)

                } else {

                        pg13::writeTable(conn = conn,
                                         schema = schema,
                                         tableName = "LOAD_LOG",
                                         load_log)
                }

                secretary::typewrite_italic(secretary::timepunch(),"\tMRCONSO Table successfully written to", schema, " and LOAD_LOG updated.")

        }



#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details https://evs.nci.nih.gov/evs-download/thesaurus-downloads
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[pg13]{send}},\code{\link[pg13]{dropTable}}
#'  \code{\link[SqlRender]{render}}
#' @rdname load_ncit
#' @family nci evs schema
#' @export
#' @importFrom secretary typewrite_italic timepunch
#' @importFrom pg13 send dropTable lsTables appendTable writeTable
#' @importFrom SqlRender render


load_ncit <-
        function(conn) {

                schema <- "nci_evs"
                tableName <- "thesaurus"

                secretary::typewrite_italic(secretary::timepunch(),"\tDownloading Thesaurus.FLAT.zip....")
                temp <- tempfile()
                download.file("https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus.FLAT.zip",
                              destfile = temp)
                unzipped_file <- unzip(temp)


                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tCreating", schema, "if it didn't exist...")
                pg13::send(conn = conn,
                           sql_statement =
                                   SqlRender::render(
                                           "CREATE SCHEMA IF NOT EXISTS @schema;",
                                           schema = schema))



                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tNCIt Table in", schema, "dropped if it existed...")
                pg13::dropTable(conn = conn,
                                schema = schema,
                                tableName = tableName)


                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tDDLing NCIt Table...")

                pg13::send(conn = conn,
                           sql_statement =
                                   SqlRender::render(
                                           "CREATE TABLE @schema.@tableName (
                                                    cui character varying(7),
                                                    owl_uri text,
                                                    ncit_code text,
                                                    ncit_concepts text,
                                                    ncit_description text,
                                                    ncit_class text,
                                                    ncit_concept_type text,
                                                    ncit_semantic_type text
                                                );", schema = schema,
                                           tableName = tableName
                                   )
                )

                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tCopying", unzipped_file, "to NCIt Table in", schema, "...")

                pg13::send(conn = conn,
                           SqlRender::render(
                                   "COPY @schema.@tableName FROM '@vocabulary_file' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;",
                                   tableName = tableName,
                                   schema = schema,
                                   vocabulary_file = file.path(getwd(), unzipped_file)))


                Sys.sleep(1)
                secretary::typewrite_italic(secretary::timepunch(),"\tCopying", unzipped_file, "to NCIt Table in", schema, "...")

                load_log <-
                        data.frame(ll_datetime = Sys.time(),
                                   ll_table =  tableName)


                Tables <- pg13::lsTables(conn = conn,
                                         schema = schema)

                if ("LOAD_LOG" %in% Tables) {

                        pg13::appendTable(conn = conn,
                                          schema = schema,
                                          tableName = "LOAD_LOG",
                                          load_log)

                } else {

                        pg13::writeTable(conn = conn,
                                         schema = schema,
                                         tableName = "LOAD_LOG",
                                         load_log)
                }

                file.remove(unzipped_file)
                unlink(temp)

                secretary::typewrite_italic(secretary::timepunch(),"\tNCIt Table successfully written to", schema, " and LOAD_LOG updated.")
        }


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
