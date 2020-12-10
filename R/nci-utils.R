#' @title
#' Execute Crawl Delay
#'
#' @export
#' @rdname


ex_crawl_delay <-
        function(crawl_delay) {
                sp <- cli::make_spinner(which = "dots10",
                                        template = "    {spin}")
                lapply(1:(crawl_delay*100), function(x) { sp$spin(); Sys.sleep(0.01) })
                sp$finish()

        }



#' @title
#' Creates NCI Schema
#'
#' @importFrom pg13 send
#' @export

start_cg <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE) {

                pg13::send(conn = conn,
                              sql_statement =
                                        "
                                        CREATE TABLE IF NOT EXISTS cancergov.cancergov_drug (
                                            cd_datetime timestamp without time zone,
                                            drug_link character varying(255),
                                            ncit_code character varying(20),
                                            drug_synonym_type character varying(25),
                                            cancergov_drug text
                                        );


                                        CREATE TABLE IF NOT EXISTS cancergov.drug_dictionary (
                                            dd_datetime timestamp without time zone,
                                            drug character varying(255),
                                            definition text
                                        );


                                        CREATE TABLE IF NOT EXISTS cancergov.drug_dictionary_log (
                                            ddl_datetime timestamp without time zone,
                                            drug_count bigint
                                        );


                                        CREATE TABLE IF NOT EXISTS cancergov.drug_link (
                                            dl_datetime timestamp without time zone,
                                            drug character varying(255),
                                            drug_link character varying(255)
                                        );


                                        CREATE TABLE IF NOT EXISTS cancergov.drug_link_ncit (
                                            dln_datetime timestamp without time zone,
                                            drug_link character varying(255),
                                            ncit_code character varying(255)
                                        );


                                        CREATE TABLE IF NOT EXISTS cancergov.drug_link_synonym (
                                            dls_datetime timestamp without time zone,
                                            drug_link character varying(255),
                                            drug_synonym_type character varying(255),
                                            drug_synonym text
                                        );


                                        CREATE TABLE IF NOT EXISTS cancergov.drug_link_url (
                                            dlu_datetime timestamp without time zone,
                                            drug_link character varying(255),
                                            drug_link_url character varying(255)
                                        );

                                        CREATE TABLE IF NOT EXISTS cancergov.ncit_synonym (
                                            ns_datetime timestamp without time zone,
                                            ncit_code character varying(255),
                                            ncit_code_url character varying(255),
                                            term text,
                                            source character varying(255),
                                            tty character varying(255),
                                            code character varying(255)
                                        );
                                        ",
                           verbose = verbose,
                           render_sql = render_sql)
        }



#' @title
#' List CancerGov Tables
#'
#' @importFrom pg13 lsTables
#' @export

list_cg_tables <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE) {
                pg13::lsTables(conn = conn,
                               schema = "cancergov",
                               verbose = verbose,
                               render_sql = render_sql)
        }

#' @title
#' Write Staging Tables to Cancergov Schema
#'
#' @importFrom pg13 writeTable dropTable
#' @export


write_cg_staging_tbl <-
        function(conn,
                 tableName,
                 data,
                 verbose = TRUE,
                 render_sql = TRUE) {


                pg13::writeTable(conn = conn,
                                        schema = "cancergov",
                                        tableName = tableName,
                                        data = data,
                                        drop_existing = TRUE,
                                 verbose = verbose,
                                 render_sql = render_sql)


                do.call(on.exit,
                        args = list(substitute(pg13::dropTable(conn = conn,
                                                          schema = "cancergov",
                                                          tableName = tableName,
                                                          verbose = verbose,
                                                          render_sql = render_sql)),
                                add = TRUE,
                                after = FALSE),
                        envir = parent.frame())
        }
