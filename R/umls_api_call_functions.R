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

#' @title
#' Search
#'
#' @param string			 A human readable term, such as ‘gestatational diabetes’, or a code from a source vocabulary, such as 11687002 from SNOMEDCT_US.
#' @param inputType			 (optional)  Specifies the data type you are using as your search parameter.
#' @param includeObsolete		 (optional)  Return content that is a result of matches on obsolete terms.
#' @param includeSuppressible		 (optional)  Return content that is a result of matches on suppressible terms.
#' @param returnIdType			 (optional)  Specifies the type of identifier you wish to retrieve.
#' @param sabs			         (optional)  Comma-separated list of source vocabularies to include in your search
#' @param searchType			 (optional)  Type of search you wish to use.
#' @param pageNumber			 (optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize			 (optional)  Whole number that specifies the number of results to include per page.
#' @details
#' '/search/current?string=fracture of carpal bone'path Retrieves CUIs for a search term and returns a JSON Object classType of searchResults
#' '/search/current?string=fracture of carpal bone&searchType=exact'path Uses ‘exact’ searching and returns a JSON Object classType of searchResults
#' '/search/current?string=fracture of carpal bone&sabs=SNOMEDCT_US&returnIdType=code'path Returns SNOMEDCT concepts associated with a search term and returns a JSON Object classType of searchResults
#' '/search/current?string=9468002&inputType=sourceUi&searchType=exact&sabs=SNOMEDCT_US'path Returns UMLS CUIs associated with a SNOMEDCT_US concept and returns a JSON Object classType of searchResults

umls_api_search <-
        function(string,
                 searchType = "normalizedString",
                 inputType = "sourceConcept",
                 includeObsolete = NULL,
                 includeSuppressible = NULL,
                 returnIdType = NULL,
                 sabs = NULL,
                 pageSize = 100,
                 sleep_time = 5) {

                baseURL <- "https://uts-ws.nlm.nih.gov/rest/search/current"

                pageNumber <- 1
                search_response <-
                        httr::GET(url = baseURL,
                                  query = list(pageSize = pageSize,
                                               pageNumber = pageNumber,
                                               string = string,
                                               searchType = searchType,
                                               inputType = inputType,
                                               includeObsolete = includeObsolete,,
                                               includeSuppressible = includeSuppressible,
                                               returnIdType = returnIdType,
                                               sabs = sabs,
                                               ticket = get_service_ticket()))

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
                                umls_api_search(string = string) %>%
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


read_endpoints_page <-
        xml2::read_html("https://documentation.uts.nlm.nih.gov/rest/home.html")

api_endpoints_table <-
read_endpoints_page %>%
        rvest::html_nodes("table") %>%
        rvest::html_table() %>%
        purrr::pluck(2) %>%
        dplyr::slice(-1) %>%
        dplyr::mutate(Path = stringr::str_replace_all(Path,
                                                      "(^.*)([{]{1}version[}]{1})(.*$)",
                                                      "\\1v1\\3"))


link2_path <-
read_endpoints_page %>%
        rvest::html_nodes("td a") %>%
        rvest::html_attr("href") %>%
        unique()

link2_responses <- list()

for (i in 1:length(link2_path)) {

        url <-
        httr::modify_url(url = "https://documentation.uts.nlm.nih.gov/rest/",
                         path = link2_path[i])
        Sys.sleep(5)
        link2_responses[[i]] <- police::try_catch_error_as_null(xml2::read_html(url))
        Sys.sleep(5)
}
names(link2_responses) <- link2_path

link2_responses2 <- list()

for (i in 1:length(link2_path)) {

         url <- paste0("https://documentation.uts.nlm.nih.gov/rest/", link2_path[i])

         Sys.sleep(10)

         link2_responses2[[i]] <- police::try_catch_error_as_na(xml2::read_html(url))
         names(link2_responses2)[i] <- url

         Sys.sleep(5)
}

link2_responses2$`https://documentation.uts.nlm.nih.gov/rest/authentication.html` <- NULL
set2 <-
        link2_responses2 %>%
        purrr::keep(~length(.)==2) %>%
        purrr::map(function(x)
                        x %>%
                           rvest::html_nodes("table") %>%
                           rvest::html_table() %>%
                           purrr::set_names(c("Retrieval", "Query Parameters")))


script_front_matter <- list()
for (i in 1:length(set2)) {
        input <- set2[[i]]$`Query Parameters`
        page_basename <- basename(names(set2)[i])
        input <-
        input %>%
                dplyr::mutate(ParamDescription = ifelse(`Required? Y/N` == "N",
                                                   paste("(optional) ", Description),
                                                   Description)) %>%
                dplyr::mutate(FunctionArgs = ifelse(`Required? Y/N` == "N",
                                                    paste0(`Parameter name`, " = NULL"),
                                                    `Parameter name`))


        parameter_names <- input$`Parameter name`
        parameter_descriptions <- input$ParamDescription
        function_arguments <- input$FunctionArgs


        input_b <- set2[[i]]$Retrieval
        roxygen_details <-
                input_b %>%
                dplyr::mutate(`Sample URI` = paste0("'",`Sample URI`, "'path")) %>%
                tidyr::unite(col = Details, `Sample URI`, Description, sep = " ") %>%
                tidyr::unite(col = Details, Details, `Returned JSON Object classType`, sep = " and returns a JSON Object classType of ") %>%
                unlist() %>%
                paste(collapse = "\n") %>%
                purrr::map(~paste0("#' @details \n", .)) %>%
                unlist()

        roxygen_title <-
                paste0("#' @title ", stringr::str_to_title(string = stringr::str_replace_all(page_basename, "[[:punct:]]", ' ')))


        roxygen_params <-
                parameter_names %>%
                purrr::map2(parameter_descriptions,
                            function(x,y) paste0("#' @param ", x, "\t\t\t", y)) %>%
                unlist()


        new_function_name <- page_basename
        new_function_name <- stringr::str_replace_all(basename(new_function_name), "[[:punct:]]", '_')
        new_function_name <- paste0("lookup_", new_function_name)

        declaration <-
        paste0(new_function_name, " <- \n\t\tfunction(\n\t\t\t", paste(function_arguments, collapse = ",\n\t\t\t"), ") { \n\n")


        commented_paths <-
                input_b %>%
                dplyr::transmute(commented_paths = paste0("## ", `Sample URI`)) %>%
                unlist() %>%
                paste(collapse = ", ")

        query_parameters <-
                parameter_names %>%
                purrr::map(function(x) paste0(x, " = ", x)) %>%
                unlist() %>%
                paste(collapse = ",\n")

        script_block_1 <-
                paste0(
                        "link_response <- httr::GET(url = baseURL,
                        path = ", commented_paths, "
                        query = list(", query_parameters,
                        ")
                        )")

        script_front_matter[[i]] <- c(roxygen_title,
                                      roxygen_params,
                                      roxygen_details,
                                      "\n\n",
                                      declaration,
                                      script_block_1,
                                      "}") %>%
                paste0(collapse = "\n")

        names(script_front_matter)[i] <- names(set2)[i]

}





link_response <-
        httr::GET(url = baseURL,
                  path =
        )



read_endpoints_page %>%
        rvest::html_nodes("td a :last-child")


rate_limit_response <-
        httr::GET(url = "https://uts-ws.nlm.nih.gov/rest/search/current",
                  path = "/rate_limit")

rate_limit_response %>%
        httr::content() %>%
        rvest::html_nodes("div") %>%
        rvest::html_text()
