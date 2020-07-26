#' Get Individual Drug Data
#' @description
#' @importFrom purrr map
#' @importFrom centipede no_blank
#' @import dplyr bind_rows
#' @export



getDrugConcept <-
    function(conn,
             max_page) {


                .input <- loadScrapedDrugs(max_page = max_page)

                .output <-
                    .input %>%
                            purrr::map(function(x) x %>%
                                                        strsplit(split = "[ ]{0,}\r\n[ ]{0,}") %>%
                                                        unlist() %>%
                                                        centipede::no_blank() %>%
                                                        t() %>%
                                                        as.data.frame()) %>%
                            dplyr::bind_rows()

                return(.output)
    }
