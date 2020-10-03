#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @param string PARAM_DESCRIPTION
#' @param schema PARAM_DESCRIPTION, Default: 'omop_drug_to_umls_api'
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{send}},\code{\link[pg13]{appendTable}}
#'  \code{\link[dplyr]{distinct}}
#' @rdname log_umls_search
#' @export
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery send appendTable
#' @importFrom dplyr distinct

log_umls_search <-
        function(conn,
                 string,
                 schema = "omop_drug_to_umls_api") {

                # concept <- "Dexagenta"

                Schemas <- pg13::lsSchema(conn = conn)

                if (!(schema %in% Schemas)) {

                        pg13::createSchema(conn = conn,
                                           schema = schema)

                }

                Tables <- pg13::lsTables(conn = conn,
                                         schema = schema)

                if ("SEARCH_LOG" %in% Tables) {

                        current_search_result <-
                                pg13::query(conn = conn,
                                            sql_statement = pg13::buildQuery(schema = schema,
                                                                             tableName = "SEARCH_LOG",
                                                                             whereInField = "string",
                                                                             whereInVector = string))


                        proceed <- nrow(current_search_result) == 0

                } else {
                        proceed <- TRUE

                }

                if (proceed) {

                        output <-
                                umls_api_search(string = string) %>%
                                dplyr::distinct()

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = schema)

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = "omop_drug_to_umls_api")

                        if (!("SEARCH_LOG" %in% Tables)) {

                                pg13::send(conn = conn,
                                           sql_statement =
                                                   "CREATE TABLE omop_drug_to_umls_api.search_log (
                                                        search_datetime timestamp without time zone,
                                                        concept_id integer,
                                                        concept character varying(255),
                                                        string character varying(255),
                                                        searchtype character varying(255),
                                                        classtype character varying(255),
                                                        pagesize integer,
                                                        pagenumber integer,
                                                        ui character varying(255),
                                                        rootsource character varying(255),
                                                        uri character varying(255),
                                                        name character varying(255)
                                                )
                                                ;
                                                ")
                        }

                        pg13::appendTable(conn = conn,
                                          schema = "omop_drug_to_umls_api",
                                          tableName = "SEARCH_LOG",
                                          results)

                }


        }
