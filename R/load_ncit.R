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
#' @family nci
#' @export
#' @importFrom pg13 send dropTable
#' @importFrom SqlRender render


load_ncit <-
        function(conn) {

                schema <- "nci_evs"
                tableName <- "thesaurus"

                temp <- tempfile()
                download.file("https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Thesaurus.FLAT.zip",
                              destfile = temp)
                unzipped_file <- unzip(temp)


                pg13::send(conn = conn,
                           sql_statement =
                                   SqlRender::render(
                                           "CREATE SCHEMA IF NOT EXISTS @schema;",
                                           schema = schema))

                pg13::dropTable(conn = conn,
                                schema = schema,
                                tableName = tableName)

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


                pg13::send(conn = conn,
                                SqlRender::render(
                                "COPY @schema.@tableName FROM '@vocabulary_file' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;",
                                tableName = tableName,
                                schema = schema,
                                vocabulary_file = file.path(getwd(), unzipped_file)))


                file.remove(unzipped_file)
                unlink(temp)

        }

