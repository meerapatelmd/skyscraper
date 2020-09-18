#' @title
#' Scrape a Drug HTML Page
#'
#' @description
#' Scrape the Tables in the Drug Page of the NCI Drug Dictionary if one exists
#'
#' @param df                    Data Frame of any dimensions with a 'Drug' and 'Drug_Def_Link' fields that correspond to the drug and its hyperlink
#' @param random                If TRUE, will sort the df parameter randomly before executing. This is helpful when running the scrape simulataneously to save time, with one job scraping random and the other scraping in the order that it was provided in df. Default: TRUE
#' @param progress_bar          If TRUE, a progress bar is returned in the console if the function is being run interactively. Default: TRUE
#'
#' @return
#' The scrape results are cached using the cacheScrape function by Drug HTML. Other than the progress messages in the console, nothing is returned. Regardless of whether or not the entire length of the drug links have been cached, any cached results can be loaded usng the loadCachedDrugPage function.
#'
#' @details
#' The link to the drug page is scraped as a dataframe that by default are named "X1" and "X2". "X1" is the type of synonym on the drug page while "X2" is the value.
#' If an error is encountered at time of scraping, the values of both X1 and X2 default to NA. If no values were returned on scrape, the scrape returns a NULL. All scrape results are cached using the R.cache package with the directory set to "skyscraper"
#'
#' @seealso
#'  \code{\link[dplyr]{select_all}}
#'  \code{\link[tibble]{rownames}},\code{\link[tibble]{c("tibble", "tibble")}}
#'  \code{\link[progress]{progress_bar}}
#'  \code{\link[secretary]{typewrite}},\code{\link[secretary]{character(0)}}
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#' @export
#' @importFrom dplyr rename_all
#' @importFrom tibble rowid_to_column tibble
#' @importFrom progress progress_bar
#' @importFrom secretary typewrite redTxt
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_table
#' @importFrom magrittr %>%

get_drug_link_synonym <-
    function(conn,
             sleep_time = 5) {



            cgTables <- pg13::lsTables(conn = conn,
                                       schema = "cancergov")


            if ("DRUG_LINK_SYNONYM" %in% cgTables) {
                    current_drug_link_synonym_table <-
                            pg13::query(conn = conn,
                                        pg13::buildQuery(distinct = TRUE,
                                                         schema = "cancergov",
                                                         tableName = "drug_link_synonym"))

                    drug_link_table0 <-
                            pg13::readTable(conn = conn,
                                            schema = "cancergov",
                                            tableName = "drug_link")


                    drug_link_table <-
                            dplyr::left_join(drug_link_table0,
                                             current_drug_link_synonym_table,
                                             by = "drug_link") %>%
                            dplyr::filter(is.na(dls_datetime)) %>%
                            dplyr::select(all_of(colnames(drug_link_table0)))


            } else {
                    drug_link_table <-
                            pg13::readTable(conn = conn,
                                            schema = "cancergov",
                                            tableName = "drug_link")
            }



            drug_links <- drug_link_table$drug_link


            while (length(drug_links)) {


                    #drug_link <- "https://www.cancer.gov/publications/dictionaries/cancer-drug/def/792667"

                    drug_link <- drug_links[1]


                    response <-
                            police::try_catch_error_as_null(
                                xml2::read_html(drug_link)
                            )

                    Sys.sleep(sleep_time)

                    if (is.null(response)) {

                            response <<-
                                    police::try_catch_error_as_null(
                                            xml2::read_html(drug_link)
                                    )


                            Sys.sleep(sleep_time)

                    }



                    if (!is.null(response)) {


                            xml2::xml_find_all(response, ".//br") %>% xml2::xml_add_sibling("p", "\n")

                            xml2::xml_find_all(response, ".//br") %>% xml2::xml_remove()


                            results <-
                                    police::try_catch_error_as_null(
                                    response %>%
                                            rvest::html_nodes("table") %>%
                                            rvest::html_table()
                                    )


                            if (length(results)) {

                                    output  <-
                                            dplyr::bind_rows(results) %>%
                                            tidyr::separate_rows(X2,
                                                                 sep = "\n") %>%
                                            dplyr::transmute(dls_datetime = Sys.time(),
                                                             drug_link = drug_link,
                                                             X1,
                                                             X2) %>%
                                            dplyr::filter_at(vars(X1,X2),
                                                             ~nchar(.) < 255)



                                    cgTables <- pg13::lsTables(conn = conn,
                                                              schema = "cancergov")

                                    if ("DRUG_LINK_SYNONYM" %in% cgTables) {

                                            pg13::appendTable(conn = conn,
                                                              schema = "cancergov",
                                                              tableName = "drug_link_synonym",
                                                              output)


                                    } else {
                                            pg13::writeTable(conn = conn,
                                                             schema = "cancergov",
                                                             tableName = "drug_link_synonym",
                                                             output)
                                    }


                            } else {
                                    output <-
                                            data.frame(dls_datetime = Sys.time(),
                                                       drug_link = drug_link,
                                                       X1 = NA,
                                                       X2 = NA)

                                    cgTables <- pg13::lsTables(conn = conn,
                                                               schema = "cancergov")

                                    if ("DRUG_LINK_SYNONYM" %in% cgTables) {

                                            pg13::appendTable(conn = conn,
                                                              schema = "cancergov",
                                                              tableName = "drug_link_synonym",
                                                              output)


                                    } else {
                                            pg13::writeTable(conn = conn,
                                                             schema = "cancergov",
                                                             tableName = "drug_link_synonym",
                                                             output)
                                    }
                            }

                    }


                    drug_links <- drug_links[-1]


            }


            # results2 <-
            #         output %>%
            #         dplyr::bind_rows(.id = "drug_link") %>%
            #         dplyr::mutate(dls_datetime = Sys.time()) %>%
            #         dplyr::select(dls_datetime,
            #                       drug_link,
            #                       dplyr::everything())
            #
            #
            # if ("DRUG_LINK_SYNONYM" %in% cgTables) {
            #
            #         pg13::appendTable(conn = conn,
            #                           schema = "cancergov",
            #                           tableName = "drug_link_synonym",
            #                           results2)
            #
            #
            # } else {
            #         pg13::writeTable(conn = conn,
            #                           schema = "cancergov",
            #                           tableName = "drug_link_synonym",
            #                           results2)
            # }

    }

