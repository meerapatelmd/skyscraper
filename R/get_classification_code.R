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
#' @rdname scrapeRN
#' @export
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom rvest html_nodes html_text
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr transmute distinct mutate bind_rows mutate_at
#' @importFrom stringr str_remove_all
#' @importFrom centipede strsplit
#' @importFrom purrr map2 set_names map
#' @importFrom magrittr %>%

get_classification_code <-
        function(conn,
                 rn_url,
                 response,
                 sleep_time = 3) {

                # https://chem.nlm.nih.gov/chemidplus/rn/83-38-5
                # https://chem.nlm.nih.gov/chemidplus/rn/97232-34-3
                # https://chem.nlm.nih.gov/chemidplus/rn/499313-74-3
                # https://chem.nlm.nih.gov/chemidplus/rn/90106-68-6
                # https://chem.nlm.nih.gov/chemidplus/rn/225234-03-7
                # https://chem.nlm.nih.gov/chemidplus/rn/57197-43-0
                # https://chem.nlm.nih.gov/chemidplus/rn/95-30-7
                # https://chem.nlm.nih.gov/chemidplus/rn/3819-76-9
                # https://chem.nlm.nih.gov/chemidplus/rn/58911-04-9
                # https://chem.nlm.nih.gov/chemidplus/rn/135669-44-2
                # https://chem.nlm.nih.gov/chemidplus/rn/142192-09-4
                # https://chem.nlm.nih.gov/chemidplus/rn/995-32-4
                # https://chem.nlm.nih.gov/chemidplus/rn/1660-95-3
                # https://chem.nlm.nih.gov/chemidplus/rn/2666-14-0
                # https://chem.nlm.nih.gov/chemidplus/rn/2809-20-3
                # https://chem.nlm.nih.gov/chemidplus/rn/816143-80-9
                # https://chem.nlm.nih.gov/chemidplus/rn/37357-69-0
                # https://chem.nlm.nih.gov/chemidplus/rn/518-20-7
                # https://chem.nlm.nih.gov/chemidplus/rn/5117216-75-8
                # https://chem.nlm.nih.gov/chemidplus/rn/105026-49-1
                # https://chem.nlm.nih.gov/chemidplus/rn/105026-50-4
                # https://chem.nlm.nih.gov/chemidplus/rn/5105026-51-5
                # https://chem.nlm.nih.gov/chemidplus/rn/ [INN:NF]127-58-2
                # https://chem.nlm.nih.gov/chemidplus/rn/50-21-5
                # https://chem.nlm.nih.gov/chemidplus/rn/621-42-1
                # https://chem.nlm.nih.gov/chemidplus/rn/50-33-9
                # https://chem.nlm.nih.gov/chemidplus/rn/934016-19-0
                # https://chem.nlm.nih.gov/chemidplus/rn/56-81-5
                # https://chem.nlm.nih.gov/chemidplus/rn/57-91-0
                # https://chem.nlm.nih.gov/chemidplus/rn/1939126-74-5
                # https://chem.nlm.nih.gov/chemidplus/rn/1208255-63-3
                # https://chem.nlm.nih.gov/chemidplus/rn/131740-09-5
                # https://chem.nlm.nih.gov/chemidplus/rn/59-01-8
                # https://chem.nlm.nih.gov/chemidplus/rn/570406-98-3
                # https://chem.nlm.nih.gov/chemidplus/rn/677007-74-8
                # https://chem.nlm.nih.gov/chemidplus/rn/699313-73-2
                # https://chem.nlm.nih.gov/chemidplus/rn/143491-54-7
                # https://chem.nlm.nih.gov/chemidplus/rn/25174336-60-0
                # https://chem.nlm.nih.gov/chemidplus/rn/145199-73-1
                # https://chem.nlm.nih.gov/chemidplus/rn/17795-21-0
                # https://chem.nlm.nih.gov/chemidplus/rn/155775-04-5
                # https://chem.nlm.nih.gov/chemidplus/rn/12674-15-6

                # conn <- chariot::connectAthena()
                # rn_url <- "https://chem.nlm.nih.gov/chemidplus/rn/58911-04-9"

                if (missing(response)) {

                        response <- xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))
                        Sys.sleep(sleep_time)

                }

                if (!missing(conn)) {

                        connSchemas <-
                                pg13::lsSchema(conn = conn)

                        if (!("chemidplus" %in% connSchemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = "chemidplus")

                        }

                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = "chemidplus")

                        if ("CLASSIFICATION" %in% chemiTables) {

                                classification <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = "chemidplus",
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
                                tibble::as_tibble_col("concept_classification") %>%
                                dplyr::transmute(scrape_datetime = Sys.time(),
                                                 concept_classification,
                                                 rn_url = rn_url) %>%
                                dplyr::distinct()





                        if (!missing(conn)) {


                                if ("CLASSIFICATION" %in% chemiTables) {
                                        pg13::appendTable(conn = conn,
                                                          schema = "chemidplus",
                                                          tableName = "classification",
                                                          classifications)
                                } else {
                                        pg13::writeTable(conn = conn,
                                                         schema = "chemidplus",
                                                         tableName = "classification",
                                                         classifications)
                                }

                        }


                }


                if (missing(conn)) {

                       classifications

                }


        }
