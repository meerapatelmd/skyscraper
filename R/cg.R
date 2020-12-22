#' @title
#' Run CancerGov Scrape and Store
#'
#' @description
#' Run the full sequence that scrapes, parses, and stores the NCI Drug Dictionary found at CancerGov.org and any correlates to the NCI Thesaurus in a Postgres Database.
#'
#' @section
#' Web Source Types:
#' The NCI Drug Dictionary has 2 data sources that run in parallel. The first source is the Drug Dictionary itself at \href{https://www.cancer.gov/publications/dictionaries/cancer-drug}{https://www.cancer.gov/publications/dictionaries/cancer-drug}. The other source are the individual drug pages, called Drug Detail Links in skyscraper, that contain tables of synonyms, including investigational names.
#'
#' @section
#' Drug Dictionary:
#' The listed drug names and their definitions are scraped from the Drug Dictionary HTML and updated to a Drug Dictionary Table in a `cancergov` schema.
#'
#' @section
#' Drug Detail Links:
#' The links to Drug Pages are scraped from the Data Dictionary URL over the maximum page number and are saved to a Drug Link Table in the `cancergov` schema. The URLs in the Drug Link Table are then scraped for any HTML Tables of synonyms and the results are written to a Drug Link Synonym Table. The links to active clinical trials and NCIt mappings are also derived and stored in their respective tables.
#'
#'
#' @param conn Postgres connection object.
#' @param conn_fun (optional) An expr as a string that can be parsed and evaluated into a connection object. If present, it is used in lieu of the `conn` argument and disconnects the connection on exit.
#' @param steps The sequence of steps, labeled by the internal function that is called. A step can be skipped if it is removed from the list, but the order in which they are called does not change. Adding any erroneous values to this list does not have an effect. Default: c("log_drug_count", "get_dictionary_and_links", "process_drug_link_synonym", "process_drug_link_url", "process_drug_link_ncit", "get_ncit_synonym", "update_cancergov_drugs").
#' @param max_page maximum page number to iterate the scrape over in the "https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=" path, Default: 50
#'
#' @inheritParams scrape
#' @inheritParams xml2::read_html
#' @inheritParams pg13::query
#'
#' @export
#' @rdname cg_run
#' @family run functions
#' @importFrom cli cat_line cat_rule


cg_run <-
        function(conn,
                 conn_fun,
                 steps = c("log_drug_count",
                           "get_dictionary_and_links",
                           "process_drug_link_synonym",
                           "process_drug_link_url",
                           "process_drug_link_ncit",
                           "get_ncit_synonym",
                           "update_cancergov_drugs"),
                 max_page = 50,
                 sleep_time = 5,
                 encoding = "",
                 options = c("RECOVER", "NOERROR", "NOBLANKS"),
                 expiration_days = 30,
                 verbose = TRUE,
                 render_sql = TRUE) {

                on.exit(expr = closeAllConnections(),
                        add = TRUE)

                if (!missing(conn_fun)) {
                        conn <- eval(rlang::parse_expr(conn_fun))
                        on.exit(pg13::dc(conn = conn, verbose = verbose),
                                add = TRUE,
                                after = TRUE)
                }

                cli::cat_line()
                cli::cat_rule("Creating Tables")

                start_cg(conn = conn,
                         verbose = verbose,
                         render_sql = render_sql)

                if ("log_drug_count" %in% steps) {
                        cli::cat_line()
                        cli::cat_rule("Logging Drug Count")
                        log_drug_count(conn = conn,
                                       verbose = verbose,
                                       render_sql = render_sql)
                }

                if ("get_dictionary_and_links" %in% steps) {

                        cli::cat_line()
                        cli::cat_rule("Scraping the Drug Dictionary for Definitions and Links")
                        get_dictionary_and_links(conn = conn,
                                                 max_page = max_page,
                                                 sleep_time = sleep_time,
                                                 verbose = verbose,
                                                 render_sql = render_sql)

                }

                if ("process_drug_link_synonym" %in% steps) {

                        cli::cat_line()
                        cli::cat_rule("Scraping Each Drug Page for Synonyms")
                        process_drug_link_synonym(conn = conn,
                                                  sleep_time = sleep_time,
                                                  expiration_days = expiration_days,
                                                  encoding = encoding,
                                                  options = options,
                                                  verbose = verbose,
                                                  render_sql = render_sql)

                }

                if ("process_drug_link_url" %in% steps) {

                        cli::cat_line()
                        cli::cat_rule("Scraping Each Drug Page for URLs to Other Related Resources")
                        process_drug_link_url(conn = conn,
                                              sleep_time = sleep_time,
                                              expiration_days = expiration_days,
                                              encoding = encoding,
                                              options = options,
                                              verbose = verbose,
                                              render_sql = render_sql)

                }

                if ("process_drug_link_ncit" %in% steps) {

                        cli::cat_line()
                        cli::cat_rule("Extracting URLs for any NCIt Codes")
                        process_drug_link_ncit(conn = conn,
                                               verbose = verbose,
                                               render_sql = render_sql,
                                               expiration_days = expiration_days)

                }

                if ("get_ncit_synonym" %in% steps) {

                        cli::cat_line()
                        cli::cat_rule("Scraping NCIt for Synonyms Using NCIt Codes")
                        get_ncit_synonym(conn = conn,
                                         sleep_time = sleep_time,
                                         expiration_days = expiration_days,
                                         verbose = verbose,
                                         render_sql = render_sql)

                }


                if ("update_cancergov_drugs" %in% steps) {
                cli::cat_line()
                cli::cat_rule("Appending CANCERGOV_DRUGS Table with New Diffs")
                update_cancergov_drugs(conn = conn,
                                       verbose = verbose,
                                       render_sql = render_sql)
                }

        }



#' @title
#' Search Cancer.gov
#'
#' @description
#' Search the NCI Drug Dictionary using the API. Note that the results are not stored in the database in this version in the manner that ChemiDPlus (`cdp_search`) does in this package version.
#'
#' @param search_term PARAM_DESCRIPTION
#' @param size PARAM_DESCRIPTION, Default: 1000
#' @param matchType PARAM_DESCRIPTION, Default: 'Begins'
#' @param crawl_delay PARAM_DESCRIPTION, Default: 5
#' @seealso
#'  \code{\link[httr]{GET}},\code{\link[httr]{content}}
#'  \code{\link[jsonlite]{toJSON, fromJSON}}
#' @rdname cg_search
#' @export
#' @importFrom httr GET content
#' @importFrom jsonlite fromJSON

cg_search <-
        function(search_term,
                 size = 1000,
                 matchType = "Begins",
                 crawl_delay = 5) {

                Sys.sleep(crawl_delay)

                response <- httr::GET(url = "https://webapis.cancer.gov/drugdictionary/v1/Drugs/search",
                                      query = list(query = search_term,
                                                   matchType = matchType,
                                                   size = size))
                parsed <- httr::content(x = response,
                                        as = "text",
                                        encoding = "UTF-8")
                df <- jsonlite::fromJSON(txt = parsed)

                df

        }
