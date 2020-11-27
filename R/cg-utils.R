#' @title
#' Creates CancerGov Schema
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
