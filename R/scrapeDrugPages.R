#' Scrape Drug Page
#' @param max_page maximum page for the base url https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=
#' @import secretary typewrite_progress
#' @import xml2
#' @export

scrapeDrugPages <-
    function(max_page) {

            .input <- getDrugPageLinks(max_page = max_page)

            for (i in 1:nrow(.input)) {

                secretary::typewrite(paste0("[", Sys.time(), "]"), "\t", i, " of ", nrow(.input))

                cached <-
                loadCachedScrape(page=i,
                                 url= .input$DRUG_DEF_LINK[i],
                                 source="cancergov")

                if (is.null(cached)) {

                            .output <- police::try_catch_error_as_null(xml2::read_html(.input$DRUG_DEF_LINK[i]) %>%
                                                            rvest::html_nodes("dl") %>%
                                                            rvest::html_text())

                            if (!is.null(.output)) {
                                cacheScrape(object=.output,
                                                 page=i,
                                                 url=.input$DRUG_DEF_LINK[i],
                                                 source="cancergov")
                            }
                            Sys.sleep(.1)

                }
            }
    }


#
#
# %>%
#     rvest::html_nodes("dl") %>%
#     rvest::html_text() %>%
#     centipede::strsplit(x, "\r\n", type = "remove") %>%
#     unlist() %>%
#     trimws("both") %>%
#     centipede::no_blank()
#
#
#
# #output2 <- list()
# for (i in 6724:nrow(final_output_02_2)) {
#     output2[[i]] <- read_html(final_output_02_2$DRUG_DEF_LINK[i])
#     Sys.sleep(.1)
# }
# # saveRDS(output2,
# #         file = "output2.RDS")
#
#
# output3 <-
#     output2 %>%
#     purrr::map(html_nodes, "dl") %>%
#     purrr::map(html_text)
#
# output4 <-
#     output3 %>%
#     purrr::map(function(x) centipede::strsplit(x, "\r\n", type = "remove") %>%
#                    unlist() %>%
#                    trimws("both") %>%
#                    centipede::no_blank()) %>%
#     purrr::set_names(final_output_02_2$DRUG)
#
# output5_1 <-
#     output4 %>%
#     purrr::map(function(x) x[1])
#
# output5_2 <-
#     output4 %>%
#     purrr::map(function(x) x[2])
#
# final_output_b <-
#     output5_2 %>%
#     purrr::map(function(x) rubix::vector_to_tibble(x, new_col = "DRUG_PAGE_DESC")) %>%
#     dplyr::bind_rows(.id = "DRUG")
#
# output5_3 <-
#     output4 %>%
#     purrr::map(function(x) x[3])
#
# max_length <-
#     output4 %>%
#     purrr::map(length) %>%
#     unlist() %>%
#     max()
#
# output_extra_vars <-
#     output5_3 %>%
#     purrr::map(function(x) centipede::strsplit(x, split = "Abbreviation:|Synonym:|Code name:|Foreign brand name:|US brand name:|Chemical structure:|Acronym:",
#                                                type = "before") %>%
#                    unlist() %>%
#                    unlist()) %>%
#     purrr::map(function(x)
#         x %>%
#             purrr::map(function(y) centipede::strsplit(y, split = "Abbreviation:|Synonym:|Code name:|Foreign brand name:|US brand name:|Chemical structure:|Acronym:",
#                                                        type = "after") %>%
#                            unlist() %>%
#                            unlist()
#             ))
#
# output_extra_vars2 <-
#     output_extra_vars %>%
#     purrr::map(function(x) unlist(x))
#
# max_length <-
#     output_extra_vars2 %>%
#     purrr::map(length) %>%
#     unlist() %>%
#     max()
#
# column_indices <-
#     seq(from = 1,
#         to = max_length,
#         by = 2)
#
# value_indices <-
#     seq(from = 2,
#         to = max_length,
#         by = 2)
#
# output_extra_vars3 <-
#     output_extra_vars2 %>%
#     purrr::map(function(x) x[column_indices] %>% centipede::no_na())
#
#
# output_extra_vars3_b <-
#     output_extra_vars2 %>%
#     purrr::map(function(x) x[value_indices] %>% centipede::no_na())
#
# output_extra_vars4 <-
#     output_extra_vars3 %>%
#     purrr::map(function(x) tibble(Field = x))
#
# output_extra_vars4_b <-
#     output_extra_vars3_b %>%
#     purrr::map(function(x) tibble("Field Value" = x))
#
# output_extra_vars5 <-
#     output_extra_vars4 %>%
#     purrr::map2(output_extra_vars4_b, function(x,y) cbind(x,y))
#
#
# output_extra_vars6 <-
#     dplyr::bind_rows(output_extra_vars5,
#                      .id = "DRUG") %>%
#     dplyr::mutate_at(vars(Field), stringr::str_remove_all, "[[:punct:]]{1}$") %>%
#     tidyr::pivot_wider(names_from = Field,
#                        values_from = `Field Value`,
#                        values_fn = list(`Field Value` = function(x) paste(x, collapse = "\n")))
#
# final_output_a <-
#     output_extra_vars6
#
# final_output <-
#     dplyr::full_join(final_output_b,
#                      final_output_a)
#
# final <-
#     dplyr::full_join(final_output_01,
#                      final_output)
#
# broca::view_as_csv(final)
# #stringr::str_replace_all(unlist(output5_3), "(^.*?:)(.*$)", "\\1") %>% unique()
#
#
#
# # Removing Header Text for first value per page
# output <-
#     input %>%
#     dplyr::mutate_all(stringr::str_replace_all, "(^.*results found for: ALL )(.*$)", "\\2") %>%
#     dplyr::mutate_all(trimws)
#
# final_output_01 <- output_01_4
#
#
#
# broca::view_as_csv(final_output_01)
#
