#' @title
#' Scrape the Classification Code in the Summary Header of the RN URL
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @param rn_url PARAM_DESCRIPTION
#' @param sleep_time PARAM_DESCRIPTION, Default: 3
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate_all}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[centipede]{strsplit}}
#'  \code{\link[purrr]{map2}},\code{\link[purrr]{set_names}},\code{\link[purrr]{map}}
#' @export
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom rvest html_nodes html_text
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr transmute distinct mutate bind_rows mutate_at mutate_if
#' @importFrom stringr str_remove_all
#' @importFrom centipede strsplit
#' @importFrom purrr map2 set_names map
#' @importFrom magrittr %>%

get_classification_code <-
        function(conn,
                 rn_url,
                 response,
                 schema = "chemidplus",
                 sleep_time = 3) {

                if (missing(response)) {

                        response <- xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))
                        Sys.sleep(sleep_time)

                }

                if (!missing(conn)) {

                        connSchemas <-
                                pg13::lsSchema(conn = conn)

                        if (!(schema %in% connSchemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = schema)

                        }

                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = schema)

                        if ("CLASSIFICATION" %in% chemiTables) {

                                classification <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = schema,
                                                                                     tableName = "classification",
                                                                                     whereInField = "rn_url",
                                                                                     whereInVector = rn_url))

                        }


                }

                # Proceed if:
                # Connection was provided and no Classificaiton Table exists
                # Connection was provided and classification is nrow 0
                # No connection was provided

                if (!missing(conn)) {
                        if ("CLASSIFICATION" %in% chemiTables) {
                                proceed <- nrow(classification) == 0
                        } else {
                                proceed <- TRUE
                        }
                } else {
                        proceed <- TRUE
                }


                if (proceed) {



                        summary_headers <-
                                response %>%
                                rvest::html_nodes("#summary h2") %>%
                                rvest::html_text()


                        index_classification_code <- grep("Classification Code", summary_headers)
                        next_header <-  summary_headers[index_classification_code+1]


                        replacement_pattern <- paste0("(^.*Classification Code)(.*?)(", next_header, ".*$)")

                        classifications <-
                                response %>%
                                rvest::html_nodes("#summary") %>%
                                rvest::html_text() %>%
                                strsplit(split = "[\r\n\t]") %>%
                                unlist() %>%
                                centipede::no_blank() %>%
                                paste(collapse = "||") %>%
                                stringr::str_replace_all(pattern = replacement_pattern,
                                                         replacement = "\\2") %>%
                                strsplit(split = "[|]{2}") %>%
                                unlist() %>%
                                trimws(which = "both") %>%
                                tibble::as_tibble_col("substance_classification") %>%
                                dplyr::transmute(c_datetime = Sys.time(),
                                                 substance_classification,
                                                 rn_url = rn_url) %>%
                                dplyr::distinct() %>%
                                rubix::filter_at_grepl(substance_classification,
                                                       grepl_phrase = "^Substance Name|^Molecular|^Note",
                                                       evaluates_to = FALSE)

                        if (!missing(conn)) {


                                if ("CLASSIFICATION" %in% chemiTables) {
                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "classification",
                                                          classifications)
                                } else {
                                        pg13::writeTable(conn = conn,
                                                         schema = schema,
                                                         tableName = "classification",
                                                         classifications)
                                }

                        }


                }


                if (missing(conn)) {

                        classifications

                }


        }
