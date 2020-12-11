#' @title
#' NCI Drug Dictionary Internal Functions
#'
#' @description
#' This is an internal function to the `_run*` function, which is part of the family of functions that scrape, parse, and store the NCI Drug Dictionary found at CancerGov.org and any correlates to the NCI Thesaurus in a Postgres Database. Use \code{\link{cg_run}} to run the full sequence. See details for more info.
#'
#' @name nci_internal
NULL


#' @title
#' Get the NCI Drug Dictionary
#'
#' @inherit nci_internal description
#'
#' @details
#' Retrieve the total number of drugs in the NCI Drug Dictionary from the Drug Dictionary API (\url{https://webapis.cancer.gov/drugdictionary/v1/index.html#/Drugs/Drugs_GetByName})
#'
#' @param crawl_delay Delay in seconds.
#' @return
#' A list with results and metadata.
#'
#' @seealso
#'  \code{\link[httr]{GET}},\code{\link[httr]{content}}
#'  \code{\link[jsonlite]{toJSON, fromJSON}}
#' @rdname drug_count
#' @export
#' @importFrom httr GET content
#' @importFrom jsonlite fromJSON

get_nci_dd <-
        function(crawl_delay = 5,
                 size = 10000,
                 letters,
                 verbose = TRUE) {

                lttrs <- c(LETTERS, "%23")

                if (!missing(letters)) {
                        lttrs <- toupper(letters)
                }

                output <- list()
                for (i in seq_along(lttrs)) {

                if (verbose) {
                        secretary::typewrite_progress(iteration = i,
                                                      total = length(lttrs))
                        secretary::typewrite(secretary::blueTxt("Letter:"), lttrs[i])
                }

                ex_crawl_delay(crawl_delay = crawl_delay)
                response <- httr::GET(url = sprintf("https://webapis.cancer.gov/drugdictionary/v1/Drugs/expand/%s?", lttrs[i]),
                                      query = list(size = size))
                parsed <- httr::content(x = response,
                                        as = "text",
                                        encoding = "UTF-8")
                results <- jsonlite::fromJSON(txt = parsed)


                output[[i]] <- results

                }

                names(output) <- lttrs

                # max_records <- results$meta$totalResults
                # batches <- ceiling(max_records/size)
                #
                # indices <-
                #         seq(from = 0,
                #             to = batches*size,
                #             by = size)
                # starting <<- indices[1:batches]
                #
                # output <- list()
                # for (i in 1:batches) {
                #         ex_crawl_delay(crawl_delay = crawl_delay)
                #         response <- httr::GET(url = "https://webapis.cancer.gov/drugdictionary/v1/Drugs",
                #                               query = list(size = size,
                #                                            from = starting[i]))
                #         parsed <- httr::content(x = response,
                #                                 as = "text",
                #                                 encoding = "UTF-8")
                #         output[[i]] <- jsonlite::fromJSON(txt = parsed)
                # }

                output

                totalResults <-
                output %>%
                        purrr::transpose() %>%
                        purrr::pluck("meta") %>%
                        purrr::transpose() %>%
                        purrr::pluck("totalResults") %>%
                        purrr::map(~ tibble::as_tibble_col(., column_name = "count")) %>%
                        dplyr::bind_rows(.id = "letter")

                grandTotal <-
                        output %>%
                        purrr::transpose() %>%
                        purrr::pluck("meta") %>%
                        purrr::transpose() %>%
                        purrr::pluck("totalResults") %>%
                        unlist() %>%
                        sum()

                results <-
                        output %>%
                        purrr::transpose() %>%
                        purrr::pluck("results") %>%
                        dplyr::bind_rows(.id = "letter")

                drugInfoSummaryLink <-
                        results$drugInfoSummaryLink %>%
                        dplyr::rename(uri_text = text)

                definition <- results$definition %>%
                        dplyr::rename(html_text = text)


                aliases <-
                        results$aliases %>%
                        purrr::set_names(1:length(results$aliases)) %>%
                        purrr::keep(~ !is.null(.)) %>%
                        dplyr::bind_rows(.id = "rowid") %>%
                        dplyr::mutate(rowid = as.integer(rowid))

                results <-
                        results %>%
                        dplyr::select(-drugInfoSummaryLink,
                                      -definition,
                                      -aliases) %>%
                        dplyr::bind_cols(drugInfoSummaryLink,
                                      definition) %>%
                        tibble::rowid_to_column(var = "rowid") %>%
                        dplyr::left_join(aliases) %>%
                        dplyr::distinct()


                list(meta = list(grandTotal = grandTotal,
                                 totalResults = totalResults),
                     results = results)

                }


#' @title
#' Get the Drug Count in the Drug Dictionary
#'
#' @inherit cancergov_internal description
#'
#' @details
#' Retrieve the total number of drugs in the NCI Drug Dictionary from the Drug Dictionary API (\url{https://webapis.cancer.gov/drugdictionary/v1/index.html#/Drugs/Drugs_GetByName})
#'
#' @param size The number of records to retrieve.
#' @param crawl_delay Delay in seconds.
#' @return
#' Drug count as integer
#'
#' @seealso
#'  \code{\link[httr]{GET}},\code{\link[httr]{content}}
#'  \code{\link[jsonlite]{toJSON, fromJSON}}
#' @rdname drug_count
#' @export
#' @importFrom httr GET content
#' @importFrom jsonlite fromJSON

drug_count <-
        function(size = 10000,
                 crawl_delay = 5) {

                Sys.sleep(crawl_delay)

                response <- httr::GET(url = "https://webapis.cancer.gov/drugdictionary/v1/Drugs",
                                      query = list(size = size,
                                                   includeResourceTypes = "DrugTerm"))
                parsed <- httr::content(x = response,
                                        as = "text",
                                        encoding = "UTF-8")
                df <- jsonlite::fromJSON(txt = parsed)

                df$meta$totalResults

        }


#' @title
#' Log the Drug Count in the Drug Dictionary
#'
#' @description
#' Log the drug count scraped by \code{\link{drug_count}} with a timestamp to the Drug Dictionary Log Table and also receive a time difference and count comparison between the most recent count and the current count in the R console.
#'
#' @inheritParams cg_run
#' @seealso
#'  \code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
#'  \code{\link[secretary]{typewrite}}
#'  \code{\link[tibble]{tibble}}
#' @rdname log_drug_count
#' @export
#' @importFrom pg13 query appendTable
#' @importFrom secretary typewrite
#' @importFrom tibble tibble


log_drug_count <-
        function(conn,
                 verbose = TRUE,
                 render_sql = TRUE,
                 crawl_delay = 5) {


                nci_dd_count <- drug_count(crawl_delay = crawl_delay)

                most_recent_count <-
                        pg13::query(conn = conn,
                                    sql_statement =
                                            "
                                                SELECT ddl.ddl_datetime, ddl.drug_count
                                                FROM cancergov.drug_dictionary_log ddl
                                                WHERE ddl.ddl_datetime IN (
                                                                SELECT MAX(ddl_datetime)
                                                                FROM cancergov.drug_dictionary_log
                                                )
                                                "
                        )

                if (verbose) {
                        time_difference <- as.character(signif(Sys.time()-most_recent_count$ddl_datetime, digits = 3))
                        time_difference_units <- units(Sys.time()-most_recent_count$ddl_datetime)
                        secretary::typewrite(sprintf("Last drug count was %s and from %s %s ago",
                                                     most_recent_count$drug_count,
                                                     time_difference,
                                                     time_difference_units))

                        secretary::typewrite("Current drug count:", nci_dd_count)
                }


                pg13::appendTable(conn = conn,
                                  schema = "cancergov",
                                  tableName = "DRUG_DICTIONARY_LOG",
                                  data = tibble::tibble(ddl_datetime = Sys.time(),
                                                        drug_count = nci_dd_count))


        }
