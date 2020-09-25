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

