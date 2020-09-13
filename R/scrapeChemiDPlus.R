#' Get any synonyms in chemidplus
#' @description Helpful with investigational drugs
#' @import httr
#' @import rvest
#' @import stringr
#' @import police
#' @import centipede
#' @export

scrapeChemiDPlus <-
    function(phrase,
             type = "contains") {


        status_df <-
            tibble::tibble(phrase = phrase,
                           type = type)


        #Remove all spaces
        phrase <- stringr::str_remove_all(phrase, "\\s")


        #url <- "https://chem.nlm.nih.gov/chemidplus/name/contains/technetiumTc99m-labeledtilmanocept"
        url <- paste0("https://chem.nlm.nih.gov/chemidplus/name/", type, "/",  phrase)

        status_df <-
            status_df %>%
            dplyr::mutate(url = url)

        urlConn <- police::try_catch_error_as_null(url(url, "rb"))

        status_df <-
            status_df %>%
            dplyr::mutate(url_connection_status = ifelse(is.null(urlConn), "Error", "Success"))


        if (!is.null(urlConn)) {

            #print("718")
            resp <- xml2::read_html(url)
            Sys.sleep(1)
            close(urlConn)

            status_df <-
                status_df %>%
                dplyr::mutate(response_status = ifelse(is.null(resp), "None", "Exists"))


            if (!is.null(resp)) {

                no_records_response <-
                        resp %>%
                        rvest::html_nodes("h3") %>%
                        rvest::html_text()

                if (length(no_records_response) && grepl(pattern = "The following query produced no records", no_records_response)) {


                    status_df <-
                        status_df %>%
                        dplyr::mutate(response_status = "No records",
                                      response_type = "None",
                                      compound_match = "None",
                                      response = "None")

                } else {

                    status_df <-
                        status_df %>%
                        dplyr::mutate(response_status = "Records found",
                                      response_type = "Not checked",
                                      compound_match = "Not checked",
                                      response = "Not checked")


                }
            } else {
                status_df <-
                    status_df %>%
                    dplyr::mutate(response_status = "None",
                                  response_type = "None",
                                  compound_match = "None",
                                  response = "None")
            }
        } else {
            status_df <-
                status_df %>%
                dplyr::mutate(response_status = "None",
                              response_type = "None",
                              compound_match = "None",
                              response = "None")
        }
        closeAllConnections()

        status_df
    }






#                 # If there aren't any #names headers, it is likely that the query resulted in multiple search results and needs to be tied to an RN number
#                 qa <-
#                     resp %>%
#                     rvest::html_nodes("#names")
#
#                 # If there are 0 html_names #names, checking to see if it landed on a multiple results page
#                 if (length(qa) == 0) {
#
#                     #print("733")
#
#                     # multiple_results <-
#                     #     resp %>%
#                     #     rvest::html_nodes("") %>%
#                     #     rvest::html_text()
#
#
#                     status_df <-
#                         status_df %>%
#                         dplyr::mutate(response_type = "multiple")
#
#
#                     multiple_results <-
#                         resp %>%
#                             rvest::html_nodes(".bodytext") %>%
#                             rvest::html_text() %>%
#                             tibble::as_tibble_col(column_name = "multiple_match") %>%
#                             rubix::filter_at_grepl(multiple_match,
#                                                    grepl_phrase = "MW[:]{1} ",
#                                                    evaluates_to = FALSE) %>%
#                             tidyr::extract(col = multiple_match,
#                                            into = c("compound_match", "rn"),
#                                            regex = "(^.*?) \\[.*?\\](.*$)") %>%
#                             dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn))
#
#                     resp <-
#                         multiple_results$rn_url %>%
#                         purrr::map(~police::try_catch_error_as_null(url(., "rb"))) %>%
#                         purrr::set_names(multiple_results$compound_match) %>%
#                         purrr::keep(~!is.null(.)) %>%
#                         purrr::map(~xml2::read_html(.)) %>%
#                         purrr::map(~rvest::html_nodes(.,"#names")) %>%
#                         purrr::map(rvest::html_text) %>%
#                         purrr::map(~tibble::as_tibble_col(.,"response")) %>%
#                         dplyr::bind_rows(.id = "compound_match")
#
#                     closeAllConnections()
#
#                 } else {
#                     status_df <-
#                         status_df %>%
#                         dplyr::mutate(response_type = "not multiple")
#
#                     resp <-
#                         resp %>%
#                         rvest::html_nodes("#names") %>%
#                         rvest::html_text() %>%
#                         tibble::as_tibble_col("response") %>%
#                         dplyr::mutate(compound_match = phrase)
#
#                 }
#
#
#                 cbind(status_df,
#                       resp)
#
#             }
#
#
#
# #
# #
# #         if (!is.null(resp)) {
# #
# #             results <-
# #                 police::try_catch_error_as_na(
# #                 resp$response %>%
# #                 strsplit(split = "\n") %>%
# #                 unlist() %>%
# #                 stringr::str_remove_all("Systematic Name|Names and Synonyms|Results Name|Name of Substance|MeSH Heading|Synonyms|[^ -~]") %>%
# #                 trimws("both") %>%
# #                 centipede::no_blank() %>%
# #                 unique())
# #
# #
# #             compounds <-
# #                 resp$compound_match %>%
# #                 paste(collapse = "|")
# #
# #         }
# #         Sys.sleep(3)
# #         closeAllConnections()
# #
# #             status_df %>%
# #                 dplyr::mutate(compounds = compounds,
# #                               results = paste(results, collapse = "|"))
# #
# #         } else {
# #             status_df
# #
# #         }
#         }
#         }
#     }
#
