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

start_nci <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE) {

                pg13::send(conn = conn,
                              sql_statement =
                                        "
                                        CREATE TABLE IF NOT EXISTS cancergov.nci_drug_dictionary (
                                            ndd_datetime timestamp without time zone,
                                            letter varchar(3),
                                            preferredName text,
                                            termId bigint,
                                            drug text,
                                                firstLetter varchar(3),
                                                drug_name_type text,
                                                termNameType text,
                                                prettyUrlName text,
                                                nciConceptId text,
                                                nciConceptName text,
                                                uri text,
                                                uri_text text,
                                                html text,
                                                html_text text,
                                                drug_type text,
                                                drug_name text
                                        );
                                        ",
                           verbose = verbose,
                           render_sql = render_sql)
        }
