#' @title
#' Scrape a RN URL
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

scrapeRN <-
        function(conn,
                 rn_url,
                 sleep_time = 3) {

                # https://chem.nlm.nih.gov/chemidplus/rn/122312-54-3
                # https://chem.nlm.nih.gov/chemidplus/rn/74899-72-2
                # https://chem.nlm.nih.gov/chemidplus/rn/185243-69-0
                # https://chem.nlm.nih.gov/chemidplus/rn/134088-74-7
                # https://chem.nlm.nih.gov/chemidplus/rn/135968-09-1
                # https://chem.nlm.nih.gov/chemidplus/rn/196488-72-9
                # https://chem.nlm.nih.gov/chemidplus/rn/479198-61-3
                # https://chem.nlm.nih.gov/chemidplus/rn/124-65-2
                # https://chem.nlm.nih.gov/chemidplus/rn/134-50-9
                # https://chem.nlm.nih.gov/chemidplus/rn/1639-60-7
                # https://chem.nlm.nih.gov/chemidplus/rn/62-33-9
                # https://chem.nlm.nih.gov/chemidplus/rn/64-02-8
                # https://chem.nlm.nih.gov/chemidplus/rn/1609655-49-3
                # https://chem.nlm.nih.gov/chemidplus/rn/138360-83-5
                # https://chem.nlm.nih.gov/chemidplus/rn/12629-01-5
                # https://chem.nlm.nih.gov/chemidplus/rn/74899-71-1
                # https://chem.nlm.nih.gov/chemidplus/rn/ [INN:NF]127-58-2
                # https://chem.nlm.nih.gov/chemidplus/rn/209810-58-2
                # https://chem.nlm.nih.gov/chemidplus/rn/208265-92-3
                # https://chem.nlm.nih.gov/chemidplus/rn/121181-53-1
                # https://chem.nlm.nih.gov/chemidplus/rn/750598-07-3
                # https://chem.nlm.nih.gov/chemidplus/rn/852317-24-9
                # https://chem.nlm.nih.gov/chemidplus/rn/99283-10-0
                # https://chem.nlm.nih.gov/chemidplus/rn/267639-76-9
                # https://chem.nlm.nih.gov/chemidplus/rn/95734-82-0


                response <- xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))
                Sys.sleep(sleep_time)

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


                        classifications <-
                               response %>%
                                       rvest::html_nodes("#classifications li") %>%
                                       rvest::html_text() %>%
                               tibble::as_tibble_col("concept_classification") %>%
                               dplyr::transmute(scrape_datetime = as.character(Sys.time()),
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


                if (!missing(conn)) {

                        connSchemas <-
                                pg13::lsSchema(conn = conn)

                        if (!("chemidplus" %in% connSchemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = "chemidplus")

                        }

                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = "chemidplus")

                        if ("SYNONYMS" %in% chemiTables) {

                                synonyms <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = "chemidplus",
                                                                                     tableName = "synonyms",
                                                                                     whereInField = "rn_url",
                                                                                     whereInVector = rn_url))

                        }


                }

                # Proceed if:
                # Connection was provided and no Synonyms Table exists
                # Connection was provided and synonyms is nrow 0
                # No connection was provided

                if (!missing(conn)) {
                        if ("SYNONYMS" %in% chemiTables) {
                                proceed <- nrow(synonyms) == 0
                        } else {
                                proceed <- TRUE
                        }
                } else {
                        proceed <- TRUE
                }


                if (proceed) {


                       synonym_types <-
                               response %>%
                               rvest::html_nodes("#names h3") %>%
                               rvest::html_text()



                       synonyms_content <-
                       response %>%
                               rvest::html_nodes("#names") %>%
                               rvest::html_text()


                       synonyms_content <-
                               synonyms_content %>%
                               stringr::str_remove_all(pattern = "Names and Synonyms")


                        synonyms_content2 <- unlist(strsplit(synonyms_content, split = "[\r\n\t]"))


                       synonyms_content3 <-
                               stringr::str_remove_all(synonyms_content2, "[^ -~]")

                       synonyms_content4 <- unlist(centipede::strsplit(synonyms_content3,
                                                                       type = "after",
                                                                       split = paste(synonym_types, collapse = "|")))



                       index <- list()
                       synonym_types2 <- synonym_types
                       while (length(synonym_types)) {

                               synonym_type <- synonym_types[1]

                               index[[length(index)+1]] <-
                                         grep(synonym_type,
                                              synonyms_content4)


                               synonym_types <- synonym_types[-1]
                       }

                       index <- unlist(index)
                       ending <- c((index[-1])-1,
                                   length(synonyms_content4))

                       df <-
                       data.frame(index, ending) %>%
                               dplyr::mutate(starting = index+1)


                       synonyms <-
                               df$starting %>%
                                       purrr::map2(df$ending,
                                                   function(x,y) synonyms_content4[x:y]) %>%
                                       purrr::set_names(synonym_types2) %>%
                                       purrr::map(tibble::as_tibble_col, "concept_synonym_name") %>%
                                       dplyr::bind_rows(.id = "concept_synonym_type") %>%
                               dplyr::transmute(scrape_datetime = as.character(Sys.time()),
                                                rn_url = rn_url,
                                                concept_synonym_type,
                                                concept_synonym_name
                                                ) %>%
                               dplyr::mutate_at(vars(concept_synonym_name),
                                                ~substr(., 1, 254)) %>%
                               dplyr::distinct()



                       if (!missing(conn)) {


                               if ("SYNONYMS" %in% chemiTables) {
                                       pg13::appendTable(conn = conn,
                                                         schema = "chemidplus",
                                                         tableName = "synonyms",
                                                         synonyms)
                               } else {
                                       pg13::writeTable(conn = conn,
                                                        schema = "chemidplus",
                                                        tableName = "synonyms",
                                                        synonyms)
                               }

                       }

                }

                if (nrow(showConnections())) {
                        closeAllConnections()
                }


                if (missing(conn)) {

                        list(CLASSIFICATIONS = classifications,
                             SYNONYMS = synonyms)

                }


        }
