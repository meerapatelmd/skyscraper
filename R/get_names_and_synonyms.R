#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @inheritParams chemidplus_scraping_functions
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[centipede]{strsplit}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate_all}},\code{\link[dplyr]{distinct}}
#'  \code{\link[purrr]{map2}},\code{\link[purrr]{set_names}},\code{\link[purrr]{map}}
#'  \code{\link[tibble]{as_tibble}}
#' @rdname get_names_and_synonyms
#' @export
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom rvest html_nodes html_text
#' @importFrom stringr str_remove_all
#' @importFrom centipede strsplit
#' @importFrom dplyr mutate bind_rows transmute mutate_at distinct
#' @importFrom purrr map2 set_names map
#' @importFrom tibble as_tibble_col

get_names_and_synonyms <-
        function(conn,
                 rn_url,
                 response,
                 schema = "chemidplus",
                 sleep_time = 3) {

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

                        if ("NAMES_AND_SYNONYMS" %in% chemiTables) {

                                synonyms <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = schema,
                                                                                     tableName = "names_and_synonyms",
                                                                                     whereInField = "rn_url",
                                                                                     whereInVector = rn_url))

                        }


                }

                # Proceed if:
                # Connection was provided and no Synonyms Table exists
                # Connection was provided and synonyms is nrow 0
                # No connection was provided

                if (!missing(conn)) {
                        if ("NAMES_AND_SYNONYMS" %in% chemiTables) {
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


                       if (length(synonym_types) == 0) {
                               synonym_types <-
                               response %>%
                                       rvest::html_nodes("#names h2") %>%
                                       rvest::html_text()
                       }



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



                       if (length(synonym_types) > 1) {
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
                                               dplyr::transmute(nas_datetime = Sys.time(),
                                                                rn_url = rn_url,
                                                                concept_synonym_type,
                                                                concept_synonym_name
                                                                ) %>%
                                               dplyr::mutate_at(vars(concept_synonym_name),
                                                                ~substr(., 1, 254)) %>%
                                               dplyr::distinct()
                       } else {
                               synonyms <-
                                       data.frame(nas_datetime = Sys.time(),
                                                      rn_url = rn_url,
                                                      concept_synonym_type = "NA",
                                                      concept_synonym_name = synonyms_content4)

                       }



                       if (!missing(conn)) {


                               if ("NAMES_AND_SYNONYMS" %in% chemiTables) {
                                       pg13::appendTable(conn = conn,
                                                         schema = schema,
                                                         tableName = "names_and_synonyms",
                                                         synonyms)
                               } else {
                                       pg13::writeTable(conn = conn,
                                                        schema = schema,
                                                        tableName = "names_and_synonyms",
                                                        synonyms)
                               }

                       }

                }

                if (nrow(showConnections())) {
                        suppressWarnings(closeAllConnections())
                }


                if (missing(conn)) {

                        synonyms

                }


        }
