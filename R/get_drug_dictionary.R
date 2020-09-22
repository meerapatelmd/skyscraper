#' @title
#' Scrape the NCI Drug Dictionary
#'
#' @inherit cancergov_functions description
#' @inheritSection cancergov_functions Web Source Types
#' @inheritSection cancergov_functions Drug Dictionary
#' @inheritParams cancergov_functions
#'
#' @return
#' A Drug Dictionary Table in a `cancergov` schema if a Drug Dictionary Table didn't already exist. Otherwise, the new drugs are appended to the existing table.
#'
#' @seealso
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[secretary]{typewrite}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{keep}},\code{\link[purrr]{transpose}}
#'  \code{\link[dplyr]{bind}},\code{\link[dplyr]{mutate_all}}
#'  \code{\link[stringr]{str_replace}}
#'
#' @export
#' @rdname get_drug_dictionary
#' @family cancergov
#'
#' @importFrom tibble tibble
#' @importFrom secretary typewrite
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_text
#' @importFrom purrr map keep transpose
#' @importFrom dplyr bind_rows mutate_all
#' @importFrom stringr str_replace_all
#' @importFrom magrittr %>%



get_drug_dictionary <-
    function(conn,
             max_page = 50) {

            output <- list()

        for (i in 1:max_page) {


                page_scrape <- xml2::read_html(paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", i))

                no_data_message <-
                        page_scrape %>%
                        rvest::html_nodes("#ctl36_ctl00_resultListView_ctrl0_lblNoDataMessage") %>%
                        rvest::html_text()

                if (length(no_data_message) == 0) {

                                drugs <-
                                page_scrape %>%
                                        rvest::html_nodes(".dictionary-list a")  %>%
                                        rvest::html_text() %>%
                                        grep(pattern = "[\r\n]",
                                             invert = FALSE,
                                             value = TRUE) %>%
                                        trimws(which = "both")

                                definitions <-
                                page_scrape %>%
                                        rvest::html_nodes(".dictionary-list .definition")  %>%
                                        rvest::html_text() %>%
                                        trimws(which = "both")


                                output[[i]] <-
                                       data.frame(drug = drugs,
                                                  definition = definitions)



                        }
        }

            output <- dplyr::bind_rows(output) %>%
                    dplyr::distinct()


            cgTables <- pg13::lsTables(conn = conn,
                                       schema = "cancergov")


            if ("DRUG_DICTIONARY" %in% cgTables) {

                    current_drug_dictionary <- pg13::readTable(conn = conn,
                                                               schema = "cancergov",
                                                               tableName = "drug_dictionary")


                    new_drug_dictionary <-
                            dplyr::left_join(output,
                                             current_drug_dictionary,
                                             by = c("drug", "definition")) %>%
                            dplyr::filter(is.na(dd_datetime)) %>%
                            dplyr::transmute(dd_datetime = Sys.time(),
                                             drug,
                                             definition) %>%
                            dplyr::distinct()


                    pg13::appendTable(conn = conn,
                                      schema = "cancergov",
                                      tableName = "drug_dictionary",
                                      new_drug_dictionary )



            } else {

                    pg13::writeTable(conn = conn,
                                     schema = "cancergov",
                                     tableName = "drug_dictionary",
                                     output %>%
                                             dplyr::transmute(dd_datetime = Sys.time(),
                                                              drug,
                                                              definition))
            }

    }
