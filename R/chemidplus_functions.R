#' @title
#' ChemiDPlus Parsing Functions
#'
#' @description
#' There are 2 HTML responses involved in ChemiDPlus scraping. The first HTML response is associated with the concept search while the second is for the 1 to 5 Registry Numbers and their URLs if records were found. The ChemiDPlus parsing functions delineate the response into 3 groups: searches the resulted in no records, a single record, or multiple records (currently limited to parsing a maximum of 5 records).
#'
#'
#' @section
#' No Records:
#' If the ChemiDPlus search url response contained the phrase "The following query produced no records:", it is marked as not having any records.
#'
#' @section
#' Single Hit:
#' If a ChemiDPlus search results in a single match, the URL returned is the RN URL itself that is derived from the "h1" HTML node. For multiple matches, the "h1" HTML node returns blank.
#'
#' @section
#' Multiple Hits:
#' If a ChemiDPlus search has multiple possible matches, the landing page where the first 5 matches are listed is scraped for each of their RNs.
#'
#' @param response "xml_document" "xml_node" class object returned by calling xml2::read_html on the rn_url
#'
#' @name chemidplus_parsing_functions
NULL


#' @title
#' ChemiDPlus Scraping Functions
#'
#' @description
#' All ChemiDPlus Scraping Functions operate on a Registry Number URL (`rn_url`). The initial search is logged to a "REGISTRY_NUMBER_LOG" Table. If the RN URL is then tested for 404 Status and logged to the "RN_URL_VALIDITY" Table. The major sections found at the ChemiDPlus site are: "Names and Synonyms", "Classification", "Registry Numbers", "Links to Resources" with these sections are written to their respective tables "NAMES_AND_SYNONYMS", "CLASSIFICATION", "REGISTRY_NUMBERS", and "LINKS_TO_RESOURCES".
#'
#' @return
#' Each section is parsed by a respective skyscraper function that stores the scraped results in a table of the same name in a schema. If a connection argument is not provided, the results are returned as a dataframe in the R console.
#'
#' @section
#' Names and Synonyms:
#' The "Names and Synonyms" Section scraped results contain a Timestamp, RN URL. If the section has subheadings, the subheading is scraped as the Synonym Type along with the Synonym itself.
#' @section
#' Classification:
#' The "Classification" Section results contain a Timestamp, RN URL, and the drug classifications on the page.
#' @section
#' Links to Resources:
#' The "Links to Resources" Section derives all the HTML links to other data and web sources for the drug. The results include a Timestamp, RN URL, and the Resource Agency and its HTML link.
#' @section
#' Registry Numbers:
#' The "Registry Numbers" Section contains other identifiers for the given drug at other Agencies.
#' @section
#' Registry Number Log Table:
#' The REGISTRY_NUMBER_LOG Table is the landing table for any ChemiDPlus searches using skyscrape. It is the place where a source concept is searched based on a given set of parameters and all the possible Registry Numbers (RN) that source concept can be associated with in ChemiDPlus. The Registry Number then serves as a jump-off point from where a second RN URL is read and split based on the sections, and read into their corresponding ChemiDPlus Tables.
#'
#' The Table logs the Raw Concept, the processed version of the Concept (ie removed spaces and error-throwing characters to generate a valid search URL for the Concept), the type of search (ie equals or contains), and the final search URL used to read a search result. A series of booleans are performed to determine whether the search was performed (ie a response was received), and if the results were for any records, and if these records were saved. If an RN was found, it is included along with the full URL associated with the URL.
#'
#' @section
#' RN URL Validity Table:
#' The RN_URL_VALIDITY Table logs whether a HTTP 404 Error was recorded for a RN URL found in the REGISTRY_NUMBER_LOG Table for QA purposes.
#'
#' @param conn          Postgres connection object
#' @param rn_url        Registry number URL to read that also serves as an Identifier
#' @param response      (optional) "xml_document" "xml_node" class object returned by xml2::read_html for the `rn_url` argument. Providing a response from a single HTML read reduces the chance of encountering a HTTP 503 error when parsing multiple sections from a single URL. If a response argument is missing, a response is read. Followed by the `sleep_time` in seconds.
#' @param schema        Schema that the returned data is written to, Default: 'chemidplus'
#' @param sleep_time    If the response argument is missing, the number seconds to pause after reading the URL, Default: 3
#'
#' @name chemidplus_scraping_functions
NULL

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
                                tibble::as_tibble_col("concept_classification") %>%
                                dplyr::transmute(c_datetime = Sys.time(),
                                                 concept_classification,
                                                 rn_url = rn_url) %>%
                                dplyr::distinct() %>%
                                rubix::filter_at_grepl(concept_classification,
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
#' @importFrom tibble as_tibble_col tibble
#' @importFrom dplyr transmute distinct filter_at mutate_if
#' @importFrom magrittr %>%

get_classification <-
        function(conn,
                 rn_url,
                 response,
                 schema = "chemidplus",
                 sleep_time = 3) {


                if (missing(response)) {

                        response <- scrape_cdp(x = rn_url,
                                               sleep_time = sleep_time)

                }

                if (!missing(conn)) {


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
                                                 any_vars(nchar(.) < 255)) %>%
                                dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))



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



#' @title
#' Scrape the "Links to Resources" Section at a Registry Number URL
#'
#' @inherit chemidplus_scraping_functions description return
#'
#' @inheritSection chemidplus_scraping_functions Links to Resources
#'
#' @inheritParams chemidplus_scraping_functions
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[purrr]{map}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{filter_all}}
#'
#' @rdname get_links_to_resources
#'
#' @family chemidplus scraping
#'
#' @export
#'
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom rvest html_nodes html_attrs
#' @importFrom purrr map
#' @importFrom tibble as_tibble_row tibble
#' @importFrom dplyr bind_rows transmute filter_at mutate_if
#' @importFrom magrittr %>%

get_links_to_resources <-
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


                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = schema)

                        if ("LINKS_TO_RESOURCES"%in% chemiTables) {

                                links_to_resources <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = schema,
                                                                                     tableName = "LINKS_TO_RESOURCES",
                                                                                     whereInField = "rn_url",
                                                                                     whereInVector = rn_url))

                        }


                }

                # Proceed if:
                # Connection was provided and no Synonyms Table exists
                # Connection was provided and links_to_resources is nrow 0
                # No connection was provided

                if (!missing(conn)) {
                        if ("LINKS_TO_RESOURCES" %in% chemiTables) {
                                proceed <- nrow(links_to_resources) == 0
                        } else {
                                proceed <- TRUE
                        }
                } else {
                        proceed <- TRUE
                }


                if (proceed) {


                        # locator_types <-
                        #         response %>%
                        #         rvest::html_nodes("#locators h3") %>%
                        #         rvest::html_text()
                        #
                        #
                        # if (length(locator_types) == 0) {
                        #         locator_types <-
                        #         response %>%
                        #                 rvest::html_nodes("#locators h2") %>%
                        #                 rvest::html_text()
                        # }



                        # locators_content <-
                        # response %>%
                        #         rvest::html_nodes("#locators") %>%
                        #         rvest::html_text()
                        #
                        # locators_link <-
                        #         response %>%
                        #         rvest::html_nodes("#locators") %>%
                        #         rvest::html_text()


                        links_to_resources <-
                                response %>%
                                rvest::html_nodes("#locators a") %>%
                                rvest::html_attrs() %>%
                                purrr::map(tibble::as_tibble_row) %>%
                                dplyr::bind_rows() %>%
                                dplyr::transmute(
                                        ltr_datetime = Sys.time(),
                                        resource_agency = `data-name`,
                                        resource_link = href,
                                        rn_url = rn_url) %>%
                                dplyr::filter_at(vars(resource_link),
                                                 any_vars(nchar(.) < 255)) %>%
                                dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))




                        if (!missing(conn)) {


                                if ("LINKS_TO_RESOURCES" %in% chemiTables) {
                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "LINKS_TO_RESOURCES",
                                                          links_to_resources)
                                } else {
                                        pg13::writeTable(conn = conn,
                                                         schema = schema,
                                                         tableName = "LINKS_TO_RESOURCES",
                                                         links_to_resources)
                                }

                        }

                }

                if (nrow(showConnections())) {
                        closeAllConnections()
                }


                if (missing(conn)) {

                        links_to_resources

                }


        }



#' @title
#' Scrape the "Names and Synonyms" Section at a Registry Number URL
#'
#' @inherit chemidplus_scraping_functions description return
#'
#' @inheritSection chemidplus_scraping_functions Names and Synonyms
#'
#' @inheritParams chemidplus_scraping_functions
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[centipede]{strsplit}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate_all}},\code{\link[dplyr]{distinct}}
#'  \code{\link[purrr]{map2}},\code{\link[purrr]{set_names}},\code{\link[purrr]{map}}
#'  \code{\link[tibble]{as_tibble}}
#'
#' @rdname get_names_and_synonyms
#'
#' @family chemidplus scraping
#'
#' @export
#'
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom rvest html_nodes html_text
#' @importFrom stringr str_remove_all
#' @importFrom centipede strsplit
#' @importFrom dplyr mutate bind_rows transmute mutate_at distinct mutate_if
#' @importFrom purrr map2 set_names map
#' @importFrom tibble as_tibble_col tibble
#' @importFrom magrittr %>%

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
                                        tibble::tibble(index, ending) %>%
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
                                        dplyr::distinct() %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))
                        } else {
                                synonyms <-
                                        tibble::tibble(nas_datetime = Sys.time(),
                                                   rn_url = rn_url,
                                                   concept_synonym_type = "NA",
                                                   concept_synonym_name = synonyms_content4) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))

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



#' @title
#' Scrape the "Registry Numbers" Section at a Registry Number URL
#'
#' @inherit chemidplus_scraping_functions description return
#'
#' @inheritSection chemidplus_scraping_functions Registry Numbers
#'
#' @inheritParams chemidplus_scraping_functions
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[centipede]{strsplit}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate_all}},\code{\link[dplyr]{distinct}}
#'  \code{\link[purrr]{map2}},\code{\link[purrr]{set_names}},\code{\link[purrr]{map}}
#'  \code{\link[tibble]{as_tibble}}
#'
#' @rdname get_registry_numbers
#'
#' @family chemidplus scraping
#'
#' @export
#'
#' @importFrom xml2 read_html
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom rvest html_nodes html_text
#' @importFrom stringr str_remove str_remove_all
#' @importFrom centipede strsplit
#' @importFrom dplyr mutate bind_rows transmute mutate_at distinct mutate_if
#' @importFrom purrr map2 set_names map
#' @importFrom tibble as_tibble_col tibble
#' @importFrom magrittr %>%

get_registry_numbers <-
        function(conn,
                 rn_url,
                 response,
                 schema = schema,
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

                        if ("REGISTRY_NUMBERS" %in% chemiTables) {

                                registry_numbers <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = schema,
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
                                stringr::str_remove(pattern = "Registry Numbers")


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

                                        number_type <- number_types[1]

                                        index[[length(index)+1]] <-
                                                grep(number_type,
                                                     registry_numbers_content4)


                                        number_types <- number_types[-1]
                                }

                                index <- unlist(index)
                                ending <- c((index[-1])-1,
                                            length(registry_numbers_content4))

                                df <-
                                        tibble::tibble(index, ending) %>%
                                        dplyr::mutate(starting = index+1)


                                registry_numbers <-
                                        df$starting %>%
                                        purrr::map2(df$ending,
                                                    function(x,y) registry_numbers_content4[x:y]) %>%
                                        purrr::set_names(number_types2) %>%
                                        purrr::map(tibble::as_tibble_col, "concept_registry_number") %>%
                                        dplyr::bind_rows(.id = "concept_registry_number_type") %>%
                                        dplyr::transmute(rn_datetime = Sys.time(),
                                                         rn_url = rn_url,
                                                         concept_registry_number_type,
                                                         concept_registry_number
                                        ) %>%
                                        dplyr::mutate_at(vars(concept_registry_number),
                                                         ~substr(., 1, 254)) %>%
                                        dplyr::distinct() %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))
                        } else {
                                registry_numbers <-
                                        tibble::tibble(rn_datetime = Sys.time(),
                                                   rn_url = rn_url,
                                                   concept_registry_number_type = "NA",
                                                   concept_registry_number = registry_numbers_content4) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))

                        }



                        if (!missing(conn)) {


                                if ("REGISTRY_NUMBERS" %in% chemiTables) {
                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "REGISTRY_NUMBERS",
                                                          registry_numbers)
                                } else {
                                        pg13::writeTable(conn = conn,
                                                         schema = schema,
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



#' @title
#' Get a Response from a RN URL
#' @description FUNCTION_DESCRIPTION
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
#' @rdname get_response
#' @export
#' @importFrom xml2 read_html

get_response <-
        function(rn_url,
                 sleep_time = 3) {

                Sys.sleep(sleep_time)
               xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))
        }



#' @title
#' Check that the Registry Number URL is Valid
#'
#' @inherit chemidplus_scraping_functions description return
#'
#' @inheritSection chemidplus_scraping_functions RN URL Validity Table
#'
#' @inheritParams chemidplus_scraping_functions
#'
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[tibble]{tibble}}
#'
#' @rdname get_rn_url_validity
#'
#' @family chemidplus scraping
#'
#' @export
#'
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom dplyr mutate_if
#' @importFrom tibble tibble
#' @importFrom magrittr %>%

get_rn_url_validity <-
        function(conn,
                 rn_url,
                 response,
                 schema = "chemidplus",
                 sleep_time = 3) {


                if (!missing(conn)) {

                        connSchemas <-
                                pg13::lsSchema(conn = conn)

                        if (!(schema %in% connSchemas)) {

                                pg13::createSchema(conn = conn,
                                                   schema = schema)

                        }

                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = schema)

                        if ("RN_URL_VALIDITY" %in% chemiTables) {

                                rn_url_validity <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = schema,
                                                                                     tableName = "RN_URL_VALIDITY",
                                                                                     whereInField = "rn_url",
                                                                                     whereInVector = rn_url))

                        }

                }


                # Proceed if:
                # Connection was provided and a rn_url_validity table is present: nrow(rn_url_validity) == 0
                # Connection was provided and rn_url_validity table was not present
                # Connection was not provided
                if (!missing(conn)) {

                        if ("RN_URL_VALIDITY" %in% chemiTables) {

                                proceed <- nrow(rn_url_validity) == 0

                        } else {

                                proceed <- TRUE

                        }

                } else {

                        proceed <- TRUE

                }

                if (proceed) {


                        if (!missing(response)) {

                                if (!is.null(response)) {

                                        status_df <-
                                                tibble::tibble(rnuv_datetime = Sys.time(),
                                                               rn_url = rn_url,
                                                               is_404 = FALSE) %>%
                                                dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))


                                } else {
                                        status_df <-
                                                tibble::tibble(rnuv_datetime = Sys.time(),
                                                               rn_url = rn_url,
                                                               is_404 = is404(rn_url = rn_url)) %>%
                                                dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))
                                        Sys.sleep(sleep_time)
                                }

                        } else {

                                status_df <-
                                        tibble::tibble(rnuv_datetime = Sys.time(),
                                                       rn_url = rn_url,
                                                       is_404 = is404(rn_url = rn_url)) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))

                                Sys.sleep(sleep_time)
                        }

                        if (nrow(showConnections())) {
                                suppressWarnings(closeAllConnections())
                        }



                        if (!missing(conn)) {


                                chemiTables <-
                                        pg13::lsTables(conn = conn,
                                                       schema = schema)

                                if ("RN_URL_VALIDITY" %in% chemiTables) {

                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "RN_URL_VALIDITY",
                                                          status_df)

                                } else {

                                        pg13::writeTable(conn = conn,
                                                         schema = schema,
                                                         tableName = "RN_URL_VALIDITY",
                                                         status_df)
                                }

                        } else {

                                status_df

                        }
                }
        }


#' @title
#' Log Registry Number Matches for a Search
#'
#' @inherit chemidplus_scraping_functions description return
#'
#' @inheritSection chemidplus_scraping_functions Registry Number Log Table
#'
#' @param conn          Postgres connection object
#' @param schema        Schema that the returned data is written to, Default: 'chemidplus'
#' @param sleep_time    If the response argument is missing, the number seconds to pause after reading the URL, Default: 3
#' @param raw_concept       Character string of length 1 to be searched in ChemiDPlus
#' @param type              Type of search available at \href{https://chem.nlm.nih.gov/chemidplus/}{ChemiDPlus}, Default: "contains"
#'
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{lsTables}},\code{\link[pg13]{query}},\code{\link[pg13]{buildQuery}},\code{\link[pg13]{appendTable}},\code{\link[pg13]{writeTable}}
#'  \code{\link[dplyr]{filter}},\code{\link[dplyr]{mutate}},\code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate-joins}},\code{\link[dplyr]{distinct}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[xml2]{read_xml}}
#'
#' @rdname log_registry_number
#'
#' @family chemidplus scraping
#'
#' @export
#'
#' @importFrom pg13 lsSchema createSchema lsTables query buildQuery appendTable writeTable
#' @importFrom dplyr filter mutate bind_rows left_join distinct mutate_if
#' @importFrom stringr str_remove_all
#' @importFrom tibble tibble
#' @importFrom xml2 read_html
#' @importFrom magrittr %>%
#' @importFrom purrr keep

log_registry_number <-
        function(conn,
                 raw_concept,
                 type = "contains",
                 sleep_time = 3,
                 schema = "chemidplus") {

                # conn <- chariot::connectAthena()
                # raw_concept <- "BI 836858"
                # type <- "contains"
                # sleep_time <- 5
                # schema <- "chemidplus"
                # export_repo <- FALSE

                # conn <- chariot::connectAthena()
                # raw_concept <- "BEZ235"
                # type <- "contains"
                # sleep_time <- 5
                # schema <- "chemidplus"
                # export_repo <- FALSE

                search_type <- type


                if (!missing(conn)) {

                        chemiTables <- pg13::lsTables(conn = conn,
                                                      schema = schema)

                        if ("REGISTRY_NUMBER_LOG" %in% chemiTables) {

                                registry_number_log <-
                                        pg13::query(conn = conn,
                                                    sql_statement = pg13::buildQuery(distinct = TRUE,
                                                                                     schema = schema,
                                                                                     tableName = "REGISTRY_NUMBER_LOG",
                                                                                     whereInField = "raw_concept",
                                                                                     whereInVector = raw_concept)) %>%
                                        dplyr::filter(type == search_type) %>%
                                        dplyr::filter(response_received == "TRUE")

                        }


                }


                # Proceed if:
                # Connection was provided and a registry_number_log table is present: nrow(registry_number_log) == 0
                # Connection was provided and registry_number_log table was not present
                # Connection was not provided
                if (!missing(conn)) {

                        if ("REGISTRY_NUMBER_LOG" %in% chemiTables) {

                                proceed <- nrow(registry_number_log) == 0

                        } else {

                                proceed <- TRUE

                        }

                } else {

                        proceed <- TRUE

                }

                if (proceed) {


                        #Remove all spaces
                        processed_concept <- stringr::str_remove_all(raw_concept, "\\s|[']{1}")


                        #url <- "https://chem.nlm.nih.gov/chemidplus/name/contains/technetiumTc99m-labeledtilmanocept"

                        url <- paste0("https://chem.nlm.nih.gov/chemidplus/name/", search_type, "/",  processed_concept)


                        status_df <-
                                tibble::tibble(rnl_datetime = Sys.time(),
                                               raw_concept = raw_concept,
                                               processed_concept = processed_concept,
                                               type = search_type) %>%
                                dplyr::mutate(url = url)


                        Sys.sleep(sleep_time)

                        resp <- xml2::read_html(url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"))


                        if (!is.null(resp)) {

                                status_df <-
                                        status_df %>%
                                        dplyr::mutate(response_received = "TRUE") %>%
                                        dplyr::mutate(no_record = isNoRecord(response = resp))


                                if (!status_df$no_record) {

                                        single_hit_resultset1 <-
                                                tryCatch(isSingleHit(response = resp),
                                                         error = function(e) NULL)

                                        single_hit_resultset2 <-
                                                tryCatch(isSingleHit2(response = resp),
                                                         error = function(e) NULL)

                                        multiple_hit_resultset1 <-
                                                tryCatch(isMultipleHits(response = resp),
                                                         error = function(e) NULL)

                                        multiple_hit_resultset2 <-
                                                tryCatch(isMultipleHits2(response = resp),
                                                         error = function(e) NULL)

                                        multiple_hit_resultset3 <-
                                                tryCatch(isMultipleHits3(response = resp),
                                                         error = function(e) NULL)


                                        results <-
                                                list(single_hit_resultset1,
                                                     single_hit_resultset2,
                                                     multiple_hit_resultset1,
                                                     multiple_hit_resultset2,
                                                     multiple_hit_resultset3) %>%
                                                purrr::keep(~!is.null(.)) %>%
                                                dplyr::bind_rows() %>%
                                                dplyr::mutate(url = url)

                                        status_df <-
                                                status_df %>%
                                                dplyr::mutate(response_recorded = "TRUE") %>%
                                                dplyr::left_join(results, by = "url") %>%
                                                dplyr::distinct() %>%
                                                dplyr::filter(rn_url != "https://chem.nlm.nih.gov/chemidplus/rn/NA")


                                } else {

                                        status_df <-
                                                status_df %>%
                                                dplyr::mutate(response_recorded = "FALSE") %>%
                                                dplyr::mutate(compound_match = NA,
                                                              rn = NA,
                                                              rn_url = NA)


                                }


                        } else {

                                status_df <-
                                        status_df %>%
                                        dplyr::mutate(response_received = "FALSE",
                                                      no_record = NA,
                                                      response_recorded = "FALSE")  %>%
                                        dplyr::mutate(compound_match = NA,
                                                      rn = NA,
                                                      rn_url = NA)

                        }


                        if (nrow(showConnections())) {
                                suppressWarnings(closeAllConnections())
                        }



                        if (!missing(conn)) {



                                chemiTables <-
                                        pg13::lsTables(conn = conn,
                                                       schema = schema)

                                if ("REGISTRY_NUMBER_LOG" %in% chemiTables) {

                                        pg13::appendTable(conn = conn,
                                                          schema = schema,
                                                          tableName = "REGISTRY_NUMBER_LOG",
                                                          data = status_df %>%
                                                                  dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]")))

                                } else {

                                        pg13::writeTable(conn = conn,
                                                         schema = schema,
                                                         tableName = "REGISTRY_NUMBER_LOG",
                                                         data = status_df %>%
                                                                 dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]")))
                                }

                        } else {

                                status_df %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))

                        }
                }
        }


#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @param schema PARAM_DESCRIPTION
#' @param errors PARAM_DESCRIPTION
#' @param search_type PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[dplyr]{mutate}}
#'  \code{\link[stringr]{str_remove}}
#'  \code{\link[pg13]{dropTable}},\code{\link[pg13]{writeTable}},\code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#' @rdname log_errors
#' @export
#' @importFrom tibble tibble
#' @importFrom dplyr mutate mutate_if
#' @importFrom stringr str_remove_all
#' @importFrom pg13 dropTable writeTable query appendTable
#' @importFrom SqlRender render


log_errors <-
        function(conn,
                 schema,
                 errors,
                 search_type) {

                errors_to_rnl <-
                tibble::tibble(rnl_datetime = Sys.time(),
                               raw_concept = errors) %>%
                        dplyr::mutate(processed_concept =  stringr::str_remove_all(raw_concept, "\\s|[']{1}")) %>%
                        dplyr::mutate(url = paste0("https://chem.nlm.nih.gov/chemidplus/name/", search_type, "/",processed_concept)) %>%
                        dplyr::mutate(response_received = "ERROR",
                                      no_record = NA_character_,
                                      response_recorded = NA_character_,
                                      compound_match = NA_character_,
                                      rn = NA_character_,
                                      rn_url = NA_character_)


                temp_table_name <- paste0("v", stringr::str_remove_all(as.character(Sys.time()), "[[:punct:]]|\\s"))

                pg13::dropTable(conn = conn,
                                schema = schema,
                                tableName = temp_table_name)

                pg13::writeTable(conn = conn,
                                 schema = schema,
                                 tableName = temp_table_name,
                                 errors_to_rnl)


                new_errors_to_rnl <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                    SqlRender::render(
                                    "
                                    SELECT temp.*
                                    FROM @schema.@temp_table_name temp
                                    LEFT JOIN @schema.registry_number_log log
                                    ON LOWER(log.raw_concept) = LOWER(temp.raw_concept)
                                    AND LOWER(log.processed_concept) = LOWER(temp.processed_concept)
                                    AND LOWER(log.response_received) = LOWER(temp.response_received)
                                    WHERE log.rnl_datetime IS NULL",
                                    schema = schema,
                                    temp_table_name = temp_table_name
                                    ))


                pg13::dropTable(conn = conn,
                                schema = schema,
                                tableName = temp_table_name)

                pg13::appendTable(conn = conn,
                                  schema = schema,
                                  tableName = "registry_number_log",
                                  new_errors_to_rnl %>%
                                          dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]")))
        }


