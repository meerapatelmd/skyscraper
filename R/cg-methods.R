#' @title
#' Lookup an NCIt Code
#'
#' @description
#' Lookup an NCIt Code in the NCIt Synonym Table. If the resultset has 0 rows, a scrape of the url path to the NCIt entry is done and any parsed response is saved to the Table.
#'
#' @inheritParams cg_run
#' @param ncit_code NCI Thesaurus Code to lookup.
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[secretary]{typewrite}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#'  \code{\link[purrr]{keep}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{select_all}}
#'  \code{\link[rubix]{format_colnames}}
#' @rdname lookup_ncit_code
#' @export
#' @importFrom pg13 query appendTable
#' @importFrom SqlRender render
#' @importFrom secretary typewrite
#' @importFrom rvest html_nodes html_table
#' @importFrom purrr keep
#' @importFrom dplyr bind_rows mutate rename_all transmute
#' @importFrom rubix format_colnames

lookup_ncit_code <-
        function(ncit_code,
                 conn,
                 sleep_time = 5,
                 verbose = TRUE,
                 render_sql = TRUE,
                 encoding = "",
                 options = c("RECOVER", "NOERROR", "NOBLANKS")) {


                ncit_synonym_table <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                            SqlRender::render(
                                                    "
                                                SELECT DISTINCT
                                                        dln.*, ns.ns_datetime
                                                FROM cancergov.drug_link_ncit dln
                                                LEFT JOIN cancergov.ncit_synonym ns
                                                ON ns.ncit_code = dln.ncit_code
                                                WHERE dln.ncit_code = '@ncit_code';",
                                                    ncit_code = ncit_code),
                                    verbose = verbose,
                                    render_sql = render_sql)

                # If invalid ncit code
                if (nrow(ncit_synonym_table) == 0) {


                        if (verbose) {

                                secretary::typewrite(sprintf("ncit_code '%s' not found in the database. Scraping NCIt...", ncit_code))

                        }

                        ncit_code_url <- sprintf("https://ncithesaurus.nci.nih.gov/ncitbrowser/pages/concept_details.jsf?dictionary=NCI_Thesaurus&code=%s&ns=ncit&type=synonym&key=null&b=1&n=0&vse=null#", ncit_code)



                        response <-  scrape(x = ncit_code_url,
                                            encoding = encoding,
                                            options = options,
                                            sleep_time = sleep_time,
                                            verbose = verbose)

                        if (!is.null(response)) {

                                output <-
                                        response %>%
                                        rvest::html_nodes("table") %>%
                                        rvest::html_table(fill = TRUE) %>%
                                        purrr::keep(function(x) "Term" %in% colnames(x)) %>%
                                        dplyr::bind_rows() %>%
                                        dplyr::mutate(ncit_code = ncit_code) %>%
                                        dplyr::mutate(ncit_code_url = ncit_code_url) %>%
                                        rubix::format_colnames() %>%
                                        dplyr::rename_all(tolower) %>%
                                        dplyr::transmute(ns_datetime = Sys.time(),
                                                         ncit_code,
                                                         ncit_code_url,
                                                         term,
                                                         source,
                                                         tty = type,
                                                         code)

                                pg13::appendTable(conn = conn,
                                                  schema = "cancergov",
                                                  tableName = "ncit_synonym",
                                                  data = output)

                                if (verbose) {

                                        secretary::typewrite("Querying appended response...")

                                }

                                pg13::query(conn = conn,
                                            SqlRender::render(
                                                    "SELECT *
                                            FROM cancergov.ncit_synonym ns=
                                            WHERE ns.ncit_code = '@ncit_code'",
                                                    ncit_code = ncit_code))


                        } else {

                                if (verbose) {

                                        secretary::typewrite("No response received.")

                                }

                                ncit_synonym_table

                        }


                } else {

                        ncit_synonym_table
                }

        }

