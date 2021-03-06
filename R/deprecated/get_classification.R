#' @title
#' Scrape the "Classification" Section at a Registry Number URL
#'
#' @inherit chemidplus_scraping_functions description return
#'
#' @inheritSection chemidplus_scraping_functions Classification
#'
#' @inheritParams chemidplus_scraping_functions
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{distinct}},\code{\link[dplyr]{filter_all}}
#'
#' @rdname get_classification
#'
#' @family chemidplus scraping
#'
#' @export
#'
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom rvest html_nodes html_text
#' @importFrom tibble as_tibble_col
#' @importFrom dplyr transmute distinct filter_at
#' @importFrom magrittr %>%

get_classification <-
        function(conn,
                 rn_url,
                 response,
                 schema = "chemidplus",
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
                # rn_url <- "https://chem.nlm.nih.gov/chemidplus/rn/12674-15-6"

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


                        classifications <-
                               response %>%
                                       rvest::html_nodes("#classifications li") %>%
                                       rvest::html_text() %>%
                               tibble::as_tibble_col("concept_classification") %>%
                               dplyr::transmute(c_datetime = Sys.time(),
                                                concept_classification,
                                                rn_url = rn_url) %>%
                               dplyr::distinct()   %>%
                                dplyr::filter_at(vars(concept_classification),
                                                 any_vars(nchar(.) < 255))



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
