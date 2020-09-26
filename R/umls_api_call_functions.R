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
                                        encode = "form"
                                        )

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


search_umls_api <-
        function(string,
                 searchType = "normalizedString",
                 inputType = "sourceConcept",
                 pageSize = 100,
                 sleep_time = 5) {

                baseURL <- "https://uts-ws.nlm.nih.gov/rest/search/current"

                service_ticket <- get_service_ticket()

                pageNumber <- 1
                search_response <-
                        httr::GET(url = baseURL,
                                  query = list(pageSize = pageSize,
                                               pageNumber = pageNumber,
                                               string = string,
                                               searchType = searchType,
                                               inputType = inputType,
                                               ticket = service_ticket))

                parsed_response <-
                        search_response %>%
                        httr::content(as = "text") %>%
                        jsonlite::fromJSON()

                output <-
                        tibble::tibble(
                                search_datetime = Sys.time(),
                                concept = concept,
                                string = string,
                                searchType = searchType,
                                classType = parsed_response$result$classType,
                                pageSize = parsed_response$pageSize,
                                pageNumber = parsed_response$pageNumber
                        ) %>%
                        dplyr::bind_cols(
                                parsed_response$result$results %>%
                                        as.data.frame())

                while (!("NONE" %in% output$ui)) {


                        service_ticket <- get_service_ticket()

                        Sys.sleep(sleep_time)

                        pageNumber <- pageNumber+1

                        search_response <-
                                httr::GET(url = baseURL,
                                          query = list(pageSize = pageSize,
                                                       pageNumber = pageNumber,
                                                       string = string,
                                                       searchType = searchType,
                                                       inputType = inputType,
                                                       ticket = service_ticket))

                        parsed_response <-
                                search_response %>%
                                httr::content(as = "text", encoding = "UTF-8") %>%
                                jsonlite::fromJSON()

                        output <-
                                dplyr::bind_rows(output,
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
                                                                as.data.frame()))

                }
                return(output)
        }


log_umls_search <-
        function(conn,
                 string,
                 schema = "omop_drug_to_umls_api") {

                # concept <- "Dexagenta"

                Schemas <- pg13::lsSchema(conn = conn)

                if (!(schema %in% Schemas)) {

                        pg13::createSchema(conn = conn,
                                           schema = schema)

                }

                Tables <- pg13::lsTables(conn = conn,
                                         schema = schema)

                if ("SEARCH_LOG" %in% Tables) {

                        current_search_result <-
                                pg13::query(conn = conn,
                                            sql_statement = pg13::buildQuery(schema = schema,
                                                                             tableName = "SEARCH_LOG",
                                                                             whereInField = "string",
                                                                             whereInVector = string))


                        proceed <- nrow(current_search_result) == 0

                } else {
                        proceed <- TRUE

                }

                if (proceed) {

                        output <-
                                search_umls_api(string = string) %>%
                                dplyr::distinct()

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = schema)

                        Tables <- pg13::lsTables(conn = conn,
                                                 schema = "omop_drug_to_umls_api")

                        if (!("SEARCH_LOG" %in% Tables)) {

                                pg13::send(conn = conn,
                                           sql_statement =
                                                   "CREATE TABLE omop_drug_to_umls_api.search_log (
                                                        search_datetime timestamp without time zone,
                                                        concept_id integer,
                                                        concept character varying(255),
                                                        string character varying(255),
                                                        searchtype character varying(255),
                                                        classtype character varying(255),
                                                        pagesize integer,
                                                        pagenumber integer,
                                                        ui character varying(255),
                                                        rootsource character varying(255),
                                                        uri character varying(255),
                                                        name character varying(255)
                                                )
                                                ;
                                                ")
                        }

                        pg13::appendTable(conn = conn,
                                          schema = "omop_drug_to_umls_api",
                                          tableName = "SEARCH_LOG",
                                          results)

                }


        }


rate_limit_response <-
        httr::GET(url = "https://uts-ws.nlm.nih.gov/rest/search/current",
                  path = "/rate_limit")

rate_limit_response %>%
        httr::content() %>%
        rvest::html_nodes("div") %>%
        rvest::html_text()
