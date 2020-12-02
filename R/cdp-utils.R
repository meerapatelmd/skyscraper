#' @title
#' Scrape ChemiDPlus
#'
#' @description
#' The general \code{\link{scrape}} function is modified to scrape using the special parameters that ChemiDPlus requires to avoid errors.
#'
#' @inheritParams scrape
#'
#' @rdname scrape_cdp
#' @export


scrape_cdp <-
        function(x,
                 options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE"),
                 sleep_time = 3,
                 verbose = TRUE) {

                scrape(x = x,
                        options = options,
                       sleep_time = sleep_time,
                       verbose = verbose
                )
        }



#' @title
#' Is the RN URL returning an HTTP error 404?
#'
#' @description
#' This function gets a response from an RN URL and checks for a 404 message in the error output.
#'
#' @param       rn_url  Registry Number URL
#'
#' @return
#' logical vector of length 1
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'
#' @rdname is404
#'
#' @family chemidplus parsing
#'
#' @export
#'
#' @importFrom xml2 read_html

is404 <-
        function(rn_url) {

                # rn_url <- "https://chem.nlm.nih.gov/chemidplus/rn/startswith/499313-74-3"

                response <-
                        tryCatch(
                                xml2::read_html(rn_url, options = c("RECOVER", "NOERROR", "NOBLANKS", "HUGE")),
                                error = function(e) {
                                        return(toString(e))
                                }
                        )


                if (is.character(response)) {

                        grepl("HTTP error 404",
                              response)

                } else {

                        FALSE

                }

        }



#' @title
#' Get the RNs from a page listing the first 5 matches
#'
#' @inherit chemidplus_parsing_functions description
#'
#' @inheritSection chemidplus_parsing_functions Multiple Hits
#'
#' @inheritParams chemidplus_parsing_functions
#'
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[tibble]{as_tibble}}
#'  \code{\link[rubix]{filter_at_grepl}}
#'  \code{\link[tidyr]{extract}}
#'  \code{\link[dplyr]{mutate}},\code{\link[dplyr]{mutate_all}}
#'  \code{\link[stringr]{str_remove}}
#'
#' @rdname isMultipleHits
#'
#' @family chemidplus parsing
#'
#' @export
#'
#' @importFrom rvest html_nodes html_text
#' @importFrom magrittr %>%
#' @importFrom police try_catch_error_as_null
#' @importFrom tibble as_tibble_col as_tibble tribble
#' @importFrom rubix filter_at_grepl rm_multibyte_chars normalize_all_to_na
#' @importFrom tidyr extract
#' @importFrom dplyr mutate mutate_all filter_at distinct transmute bind_rows all_vars mutate_if
#' @importFrom stringr str_remove_all

isMultipleHits <-
        function(response) {

                # Get list of the search results that can be in the format of a. "{SubstanceName}{RNNumber}No Structure" or b. "MW: {molecular weight}"
                output <-
                        response %>%
                        rvest::html_nodes(".bodytext") %>%
                        rvest::html_text()

                # Concatenate 1 or more {SubstanceName} out of the "{SubstanceName}{RNNumber}" in a pipe-separated string to isolate RN number in output above using regex
                chem_names <-
                        response %>%
                        rvest::html_nodes(".chem-name") %>%
                        rvest::html_text() %>%
                        paste(collapse = "|")




                output_a <-
                        police::try_catch_error_as_null(
                                output  %>%
                                        tibble::as_tibble_col(column_name = "multiple_match") %>%
                                        rubix::filter_at_grepl(multiple_match,
                                                               grepl_phrase = "MW[:]{1} ",
                                                               evaluates_to = FALSE) %>%
                                        tidyr::extract(col = multiple_match,
                                                       into = c("compound_match", "rn"),
                                                       regex = paste0("(^", chem_names, ") \\[.*?\\](.*$)")) %>%
                                        dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn)) %>%
                                        dplyr::mutate_all(stringr::str_remove_all, "No Structure") %>%
                                        dplyr::filter_at(vars(compound_match,
                                                              rn),
                                                         dplyr::all_vars(!is.na(.))))


                if (is.null(output_a)) {

                        chem_name_vector <-
                                response %>%
                                rvest::html_nodes(".chem-name") %>%
                                rvest::html_text()


                        output  %>%
                                tibble::as_tibble_col(column_name = "multiple_match") %>%
                                rubix::filter_at_grepl(multiple_match,
                                                       grepl_phrase = "MW[:]{1} ",
                                                       evaluates_to = FALSE) %>%
                                dplyr::mutate(compound_match = chem_name_vector) %>%
                                dplyr::mutate(nchar_compound_name = nchar(compound_match)) %>%
                                dplyr::mutate(string_start_rn = nchar_compound_name+1) %>%
                                dplyr::mutate(total_nchar = nchar(multiple_match)) %>%
                                dplyr::mutate(rn = substr(multiple_match, string_start_rn, total_nchar)) %>%
                                dplyr::mutate_all(stringr::str_remove_all, "No Structure") %>%
                                rubix::rm_multibyte_chars() %>%
                                dplyr::filter_at(vars(compound_match,
                                                      rn),
                                                 dplyr::all_vars(!is.na(.))) %>%
                                dplyr::distinct() %>%
                                tibble::as_tibble() %>%
                                rubix::normalize_all_to_na() %>%
                                dplyr::transmute(compound_match,
                                                 rn,
                                                 rn_url = ifelse(!is.na(rn),
                                                                 paste0("https://chem.nlm.nih.gov/chemidplus/rn/", rn),
                                                                 NA))


                } else {

                        output_b <-
                                output  %>%
                                tibble::as_tibble_col(column_name = "multiple_match") %>%
                                rubix::filter_at_grepl(multiple_match,
                                                       grepl_phrase = "MW[:]{1} ",
                                                       evaluates_to = FALSE) %>%
                                tidyr::extract(col = multiple_match,
                                               into = c("compound_match", "rn"),
                                               regex = paste0("(^", chem_names, ")([0-9]{1,}[-]{1}[0-9]{1,}[-]{1}[0-9]{1,}.*$)")) %>%
                                dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn)) %>%
                                dplyr::mutate_all(stringr::str_remove_all, "No Structure") %>%
                                dplyr::filter_at(vars(compound_match,
                                                      rn),
                                                 dplyr::all_vars(!is.na(.)))

                        output <-
                                dplyr::bind_rows(output_a,
                                                 output_b)  %>%
                                dplyr::distinct()


                        if (nrow(output)) {
                                output %>%
                                        tibble::as_tibble() %>%
                                        rubix::normalize_all_to_na() %>%
                                        dplyr::transmute(compound_match,
                                                         rn,
                                                         rn_url = ifelse(!is.na(rn),
                                                                         paste0("https://chem.nlm.nih.gov/chemidplus/rn/", rn),
                                                                         NA)) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))
                        } else {
                                tibble::tribble(~compound_match, ~rn, ~rn_url)
                        }
                }

        }

#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param response PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[stringr]{str_remove}},\code{\link[stringr]{modifiers}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[dplyr]{bind}}
#' @rdname isMultipleHits2
#' @export
#' @importFrom rvest html_nodes html_text
#' @importFrom stringr str_remove_all str_remove fixed
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows mutate_if


isMultipleHits2 <-
        function(response) {

                #response <- xml2::read_html("https://chem.nlm.nih.gov/chemidplus/name/contains/DEPATUXIZUMAB")

                # Get list of the search results that can be in the format of a. "{SubstanceName}{RNNumber}No Structure" or b. "MW: {molecular weight}"
                output <-
                        response %>%
                        rvest::html_nodes(".bodytext") %>%
                        rvest::html_text() %>%
                        stringr::str_remove_all("No Structure") %>%
                        grep(pattern = "^MW[:]{1}[ ]{1}", invert = TRUE, value = TRUE)

                #  Get{SubstanceName} out of the "{SubstanceName}{RNNumber}" to isolate RN number in output above using regex
                chem_names <-
                        response %>%
                        rvest::html_nodes(".chem-name") %>%
                        rvest::html_text()

                # For each chem_name found, to match it with the appropriate vector in output above and then get the RN number
                multiple_hits_results <- list()
                for (chem_name in chem_names) {

                        rn_match <- grep(chem_name, output, value = TRUE, fixed = TRUE)
                        rn_match <- stringr::str_remove(rn_match, pattern = stringr::fixed(chem_name))

                        multiple_hits_results[[1+length(multiple_hits_results)]] <-
                                tibble::tibble(compound_match = chem_name,
                                               rn = rn_match,
                                               rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/", rn_match))

                }

                dplyr::bind_rows(multiple_hits_results) %>%
                        dplyr::mutate(rn = trimws(rn, which = "both")) %>%
                        dplyr::filter(rn != "") %>%
                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))
        }


#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param response PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[stringr]{str_remove}},\code{\link[stringr]{modifiers}}
#'  \code{\link[tibble]{tibble}}
#'  \code{\link[dplyr]{bind}}
#' @rdname isMultipleHits3
#' @export
#' @importFrom rvest html_nodes html_text
#' @importFrom stringr str_remove_all str_remove fixed
#' @importFrom tibble tibble
#' @importFrom dplyr bind_rows mutate_if


isMultipleHits3 <-
        function(response) {

                #response <- xml2::read_html("https://chem.nlm.nih.gov/chemidplus/name/contains/DEPATUXIZUMAB")

                rns <-
                        response %>%
                        rvest::html_nodes(".bodytext a") %>%
                        rvest::html_attr("href") %>%
                        stringr::str_replace_all("(^javascript[:]{1}loadChemicalIndex[(]{1}['])(.*?)([']{1}.*$)", "\\2") %>%
                        centipede::no_blank()

                if (length(rns)) {

                        rn_urls <- paste0("https://chem.nlm.nih.gov/chemidplus/rn/", rns)

                        chem_names <-
                                response %>%
                                rvest::html_nodes(".chem-name") %>%
                                rvest::html_text()

                        tibble::tibble(compound_match = chem_names,
                                       rn = rns,
                                       rn_url = rn_urls) %>%
                                dplyr::mutate(rn = trimws(rn, which = "both")) %>%
                                dplyr::filter(rn != "") %>%
                                dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))

                } else {
                        tibble::tribble(~compound_match, ~rn, ~rn_url)
                }

        }

#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param response PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[centipede]{strsplit}},\code{\link[centipede]{no_na}}
#'  \code{\link[tibble]{as_tibble}},\code{\link[tibble]{tribble}}
#'  \code{\link[tidyr]{extract}},\code{\link[tidyr]{pivot_wider}}
#'  \code{\link[dplyr]{mutate}}
#'  \code{\link[stringr]{str_remove}}
#' @rdname isSingleHit2
#' @export
#' @importFrom rvest html_nodes html_text
#' @importFrom centipede strsplit no_na
#' @importFrom tibble as_tibble_col tribble tibble
#' @importFrom tidyr extract pivot_wider
#' @importFrom dplyr transmute mutate mutate_if
#' @importFrom stringr str_remove_all

isSingleHit2 <-
        function(response) {

                # response <- xml2::read_html("https://chem.nlm.nih.gov/chemidplus/name/contains/BI836858")

                # Get list of the search results that can be in the format of a. "{SubstanceName}{RNNumber}No Structure" or b. "MW: {molecular weight}"
                output <-
                        response %>%
                        rvest::html_nodes("h1") %>%
                        rvest::html_text() %>%
                        centipede::strsplit(split = "Substance Name[:]{1}|RN[:]{1}|UNII[:]{1}|InChIKey[:]{1}|ID[:]{1}", type = "before")  %>%
                        unlist() %>%
                        centipede::no_na()

                if (length(output)) {
                        output2 <-
                                output %>%
                                centipede::strsplit(split = "Substance Name[:]{1}|RN[:]{1}|UNII[:]{1}|InChIKey[:]{1}|ID[:]{1}", type = "before") %>%
                                unlist() %>%
                                tibble::as_tibble_col(column_name = "h1") %>%
                                tidyr::extract(col = h1,
                                               into = c("identifier_type", "identifier"),
                                               regex = "(^.*?)[:]{1}(.*$)") %>%
                                tidyr::pivot_wider(names_from = identifier_type,
                                                   values_from = identifier)

                        if (!("RN" %in% colnames(output2))) {

                                if ("ID" %in% colnames(output2)) {

                                        output2 %>%
                                                dplyr::transmute(compound_match = `Substance Name`,
                                                                 rn = stringr::str_remove_all(ID, "\\s{1,}")) %>%
                                                dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn)) %>%
                                                dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))

                                }


                        } else {

                                tibble::tribble(~compound_match,
                                                ~rn,
                                                ~rn_url)
                        }


                } else {
                        tibble::tribble(~compound_match,
                                        ~rn,
                                        ~rn_url)
                }
        }





#' @title
#' Does the RN URL indicate that no records were found?
#'
#' @inherit chemidplus_parsing_functions description
#'
#' @inheritSection chemidplus_parsing_functions No Records
#'
#' @inheritParams chemidplus_parsing_functions
#'
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'
#' @rdname isNoRecord
#'
#' @family chemidplus parsing
#'
#' @export
#'
#' @importFrom rvest html_nodes html_text
#' @importFrom magrittr %>%

isNoRecord <-
        function(response) {

                result <-
                        response %>%
                        rvest::html_nodes("h3") %>%
                        rvest::html_text()

                if (length(result) == 1) {

                        if (result == "The following query produced no records:") {

                                TRUE

                        } else {
                                FALSE
                        }
                } else {
                        FALSE
                }
        }



#' @title
#' Parse the RN from a single Substance Page
#'
#'
#' @inherit chemidplus_parsing_functions description
#'
#' @inheritSection chemidplus_parsing_functions Single Hit
#'
#' @inheritParams chemidplus_parsing_functions
#'
#' @seealso
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
#'  \code{\link[centipede]{strsplit}}
#'  \code{\link[tibble]{as_tibble}},\code{\link[tibble]{tribble}}
#'  \code{\link[tidyr]{extract}},\code{\link[tidyr]{pivot_wider}}
#'  \code{\link[dplyr]{mutate}}
#'  \code{\link[stringr]{str_remove}}
#'
#' @rdname isSingleHit
#'
#' @family chemidplus parsing
#'
#' @export
#'
#' @importFrom rvest html_node html_text
#' @importFrom centipede strsplit
#' @importFrom tibble as_tibble_col tribble tibble
#' @importFrom tidyr extract pivot_wider
#' @importFrom dplyr transmute mutate mutate_if
#' @importFrom stringr str_remove_all
#' @importFrom magrittr %>%


isSingleHit <-
        function(response) {

                #response <- resp

                output <-
                        response %>%
                        rvest::html_node("h1") %>%
                        rvest::html_text()


                if (!is.na(output)) {
                        output2 <-
                                output %>%
                                centipede::strsplit(split = "Substance Name[:]{1}|RN[:]{1}|UNII[:]{1}|InChIKey[:]{1}|ID[:]{1}", type = "before") %>%
                                unlist() %>%
                                tibble::as_tibble_col(column_name = "h1") %>%
                                tidyr::extract(col = h1,
                                               into = c("identifier_type", "identifier"),
                                               regex = "(^.*?)[:]{1}(.*$)") %>%
                                tidyr::pivot_wider(names_from = identifier_type,
                                                   values_from = identifier)

                        if ("RN" %in% colnames(output2)) {

                                output2 %>%
                                        dplyr::transmute(compound_match = `Substance Name`,
                                                         rn = stringr::str_remove_all(RN, "\\s{1,}")) %>%
                                        dplyr::mutate(rn_url = paste0("https://chem.nlm.nih.gov/chemidplus/rn/",rn)) %>%
                                        dplyr::mutate_if(is.character, ~stringr::str_remove_all(., "[^ -~]"))


                        } else {

                                tibble::tribble(~compound_match,
                                                ~rn,
                                                ~rn_url)
                        }


                } else {
                        tibble::tribble(~compound_match,
                                        ~rn,
                                        ~rn_url)
                }
        }
