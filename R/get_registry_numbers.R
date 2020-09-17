#' @title
#' Scrape the Registry Numbers Section of the RN URL
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

get_registry_numbers <-
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

                        if ("REGISTRY_NUMBERS" %in% chemiTables) {

                                registry_numbers <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = "chemidplus",
                                                                                     tableName = "registry_numbers",
                                                                                     whereInField = "rn_url",
                                                                                     whereInVector = rn_url))

                        }


                }

                # Proceed if:
                # Connection was provided and no Synonyms Table exists
                # Connection was provided and registry_numbers is nrow 0
                # No connection was provided

                if (!missing(conn)) {
                        if ("REGISTRY_NUMBERS" %in% chemiTables) {
                                proceed <- nrow(registry_numbers) == 0
                        } else {
                                proceed <- TRUE
                        }
                } else {
                        proceed <- TRUE
                }


                if (proceed) {


                       number_types <-
                               response %>%
                               rvest::html_nodes("#numbers h3") %>%
                               rvest::html_text()


                       if (length(number_types) == 0) {
                               number_types <-
                               response %>%
                                       rvest::html_nodes("#numbers h2") %>%
                                       rvest::html_text()
                       }



                       registry_numbers_content <-
                       response %>%
                               rvest::html_nodes("#numbers") %>%
                               rvest::html_text()


                       registry_numbers_content <-
                               registry_numbers_content %>%
                               stringr::str_remove_all(pattern = "Registry Numbers")


                        registry_numbers_content2 <- unlist(strsplit(registry_numbers_content, split = "[\r\n\t]"))


                       registry_numbers_content3 <-
                               stringr::str_remove_all(registry_numbers_content2, "[^ -~]")

                       registry_numbers_content4 <- unlist(centipede::strsplit(registry_numbers_content3,
                                                                       type = "after",
                                                                       split = paste(number_types, collapse = "|")))



                       if (length(number_types) > 1) {
                                       index <- list()
                                       number_types2 <- number_types
                                       while (length(number_types)) {

                                               synonym_type <- number_types[1]

                                               index[[length(index)+1]] <-
                                                         grep(synonym_type,
                                                              registry_numbers_content4)


                                               number_types <- number_types[-1]
                                       }

                                       index <- unlist(index)
                                       ending <- c((index[-1])-1,
                                                   length(registry_numbers_content4))

                                       df <-
                                       data.frame(index, ending) %>%
                                               dplyr::mutate(starting = index+1)


                                       registry_numbers <-
                                               df$starting %>%
                                                       purrr::map2(df$ending,
                                                                   function(x,y) registry_numbers_content4[x:y]) %>%
                                                       purrr::set_names(number_types2) %>%
                                                       purrr::map(tibble::as_tibble_col, "concept_registry_number") %>%
                                                       dplyr::bind_rows(.id = "concept_registry_number_type") %>%
                                               dplyr::transmute(scrape_datetime = Sys.time(),
                                                                rn_url = rn_url,
                                                                concept_registry_number_type,
                                                                concept_registry_number
                                                                ) %>%
                                               dplyr::mutate_at(vars(concept_registry_number),
                                                                ~substr(., 1, 254)) %>%
                                               dplyr::distinct()
                       } else {
                               registry_numbers <-
                                       data.frame(scrape_datetime = Sys.time(),
                                                      rn_url = rn_url,
                                                      concept_registry_number_type = "NA",
                                                      concept_registry_number = registry_numbers_content4)

                       }



                       if (!missing(conn)) {


                               if ("REGISTRY_NUMBERS" %in% chemiTables) {
                                       pg13::appendTable(conn = conn,
                                                         schema = "chemidplus",
                                                         tableName = "REGISTRY_NUMBERS",
                                                         registry_numbers)
                               } else {
                                       pg13::writeTable(conn = conn,
                                                        schema = "chemidplus",
                                                        tableName = "REGISTRY_NUMBERS",
                                                        registry_numbers)
                               }

                       }

                }

                if (nrow(showConnections())) {
                        closeAllConnections()
                }


                if (missing(conn)) {

                        registry_numbers

                }


        }
