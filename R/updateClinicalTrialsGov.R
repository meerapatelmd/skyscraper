#' @title
#' Update Clinical Trial Gov Database
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#' @rdname updateClinicalTrialsGov
#' @export
#' @importFrom xml2 read_html
#' @importFrom rvest html_nodes html_children html_text
#' @importFrom magrittr %>%


updateClinicalTrialsGov <-
        function() {
                aact_page <- xml2::read_html("https://aact.ctti-clinicaltrials.org/snapshots")

                file_archive <-
                        aact_page %>%
                        rvest::html_nodes(".file-archive td") %>%
                        rvest::html_children() %>%
                        rvest::html_text()
                file_archive <- file_archive[1]

                download.file(paste0("https://aact.ctti-clinicaltrials.org/static/static_db_copies/daily/", file_archive),
                              destfile = file_archive)

                unzip(file_archive)

                system(command = paste0("pg_restore -e -v -O -x -d aact --clean --no-owner ", getwd(), "/postgres_data.dmp"))

                file.remove("schema_diagram.png",
                            "admin_schema_diagram.png",
                            "nlm_results_definitions.html",
                            "nlm_protocol_definitions.html",
                            "postgres_data.dmp",
                            "data_dictionary.xlsx",
                            file_archive)
        }
