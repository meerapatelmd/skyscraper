#' @title
#' Scrape the Names and Synonyms Section of the RN URL
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

get_names_and_synonyms <-
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
                # rn_url <- "https://chem.nlm.nih.gov/chemidplus/rn/12674-15-6"

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

                        if ("NAMES_AND_SYNONYMS" %in% chemiTables) {

                                synonyms <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = "chemidplus",
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
                                                         schema = "chemidplus",
                                                         tableName = "names_and_synonyms",
                                                         synonyms)
                               } else {
                                       pg13::writeTable(conn = conn,
                                                        schema = "chemidplus",
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
