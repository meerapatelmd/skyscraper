#' @title
#' Functions that Make and Parse API Calls
#' @description




auth_umls_api <-
        function(store_creds_at = "~") {

                tgt_file_path <- file.path(store_creds_at, ".umls_api_tgt.txt")

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

                } else {
                        TGT <- readLines(tgt_file_path)
                }


if (!file.exists("~/.umls_auth_service_ticket.txt")) {
        SERVICE_TICKET <- system(paste0("curl -X POST https://utslogin.nlm.nih.gov/cas/v1/tickets/",
                                        TGT, " -H 'content-type: application/x-www-form-urlencoded' -d service=http%3A%2F%2Fumlsks.nlm.nih.gov"),
                                 intern = TRUE)
        cat(paste(rubix::stamped(), "\t", SERVICE_TICKET), file = "~/.umls_auth_service_ticket.txt")
        return(SERVICE_TICKET)
}
else {
        most_recent_SERVICE_TICKET <- readr::read_tsv("~/.umls_auth_service_ticket.txt",
                                                      col_names = c("TIMESTAMP", "SERVICE_TICKET"), col_types = c("Tc"))
        if (lubridate::make_difftime((lubridate::ymd_hms(rubix::stamped()) -
                                      most_recent_SERVICE_TICKET$TIMESTAMP), units = "minutes") >=
            4) {
                SERVICE_TICKET <- system(paste0("curl -X POST https://utslogin.nlm.nih.gov/cas/v1/tickets/",
                                                TGT, " -H 'content-type: application/x-www-form-urlencoded' -d service=http%3A%2F%2Fumlsks.nlm.nih.gov"),
                                         intern = TRUE)
                cat(paste(rubix::stamped(), "\t", SERVICE_TICKET),
                    file = "~/.umls_auth_service_ticket.txt")
                return(SERVICE_TICKET)
        }
        else {
                SERVICE_TICKET <- most_recent_SERVICE_TICKET$SERVICE_TICKET
                return(SERVICE_TICKET)
        }
}
}

