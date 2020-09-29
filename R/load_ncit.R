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

