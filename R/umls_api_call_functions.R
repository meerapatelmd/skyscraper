#' @title
#' Functions that Make and Parse API Calls
#' @description




get_service_ticket <-
        function(store_creds_at = "~") {

                tgt_file_path <- file.path(store_creds_at, ".umls_api_tgt.txt")
                service_ticket_file_path <- file.path(store_creds_at, ".umls_api_service_ticket.txt")

                if (file.exists(tgt_file_path)) {
                        if (Sys.time() - file.info(tgt_file_path)$mtime > 480) {
                                proceed <- TRUE
                        } else {
                                proceed <- FALSE
                        }
                } else {
                        proceed <- TRUE
                }


                if (proceed) {

                        auth_response <-
                        httr::POST(
                                url = "https://utslogin.nlm.nih.gov/cas/v1/api-key",
                                body = list(apikey = Sys.getenv("nih_api_key")),
                                encode = "form")

                        TGT <-
                        httr::content(auth_response, type = "text/html", encoding = "UTF-8") %>%
                                rvest::html_nodes("form") %>%
                                rvest::html_attr("action")

                        cat(TGT,
                            sep = "\n",
                            file = tgt_file_path)

                        tgt_response <-
                                httr::POST(
                                        url = TGT,
                                        body = list(service = "http://umlsks.nlm.nih.gov"),
                                        encode = "form")

                        service_ticket <-
                        tgt_response %>%
                                httr::content(type = "text/html", encoding = "UTF-8") %>%
                                rvest::html_text()

                        cat(service_ticket,
                            sep = "\n",
                            file = service_ticket_file_path)

                } else {

                        TGT <- readLines(tgt_file_path)


                        if (file.exists(service_ticket_file_path)) {
                                if (Sys.time() - file.info(service_ticket_file_path)$mtime >= 4) {
                                        proceed2 <- TRUE
                                } else {
                                        proceed2 <- FALSE
                                }
                        } else {
                                proceed2 <- TRUE
                        }

                        if (proceed2) {

                                tgt_response <-
                                        httr::POST(
                                                url = TGT,
                                                body = list(service = "http://umlsks.nlm.nih.gov"),
                                                encode = "form")

                                service_ticket <-
                                        tgt_response %>%
                                        httr::content(type = "text/html", encoding = "UTF-8") %>%
                                        rvest::html_text()

                                cat(service_ticket,
                                    sep = "\n",
                                    file = service_ticket_file_path)
                        } else {
                                service_ticket <- readLines(service_ticket_file_path)
                        }

                }

                file.remove(service_ticket_file_path)
                service_ticket
        }


# https://documentation.uts.nlm.nih.gov/rest/home.html

search_umls <-
        function(string,
                 searchType = c("exact", "words", "leftTruncation",
                                "rightTruncation", "approximate", "normalizedString",
                                "normalizedWords")) {

                stopifnot(length(searchType)==1)

                baseURL <- "https://uts-ws.nlm.nih.gov/rest/search/current"
                string <- URLencode(string)

                service_ticket <- get_service_ticket()

                search_response <-
                httr::GET(url = baseURL,
                          query = list(string = string,
                                        searchType = searchType,

                                        ticket = service_ticket))

                parsed_response <-
                search_response %>%
                        httr::content(as = "text") %>%
                        jsonlite::fromJSON()

                output <-
                        tibble::tibble(
                                search_datetime = Sys.time(),
                                string = string,
                                searchType = searchType,
                                classType = parsed_response$result$classType,
                                pageSize = parsed_response$pageSize,
                                pageNumber = parsed_response$pageNumber
                        ) %>%
                        dplyr::bind_cols(
                        parsed_response$result$results %>%
                                as.data.frame())


        }

