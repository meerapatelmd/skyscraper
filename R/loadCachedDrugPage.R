#' @title
#' Load Cached Drug Page
#'
#' @description
#' Regardless of whether or not the scraping has been completed, load all cached Drug Page scrapes.
#'
#' @param df Data Frame of any dimensions with a 'Drug' and 'Drug_Def_Link' fields that correspond to the drug and its hyperlink
#' @param output.var Name of output list from all the loaded cached results, Default: 'loadCachedDrugPage_results'
#' @return
#' Named list of all the cache results by drug link.
#' @details
#' This function bookmarks the process if it is stopped before completion. If a complete reset is desired, either delete the output.var object in the Global Environment or state "n" at prompt "output.var already exists. Continue? ". All other values indicate continuing from the length of the output.var.
#' @seealso
#'  \code{\link[dplyr]{select_all}},\code{\link[dplyr]{bind}}
#'  \code{\link[secretary]{typewrite}},\code{\link[secretary]{typewrite_bold}}
#' @rdname loadCachedDrugPage
#' @export
#' @importFrom dplyr rename_all bind_rows
#' @importFrom secretary typewrite typewrite_bold
#' @importFrom magrittr %>%

loadCachedDrugPage <-
    function(df, output.var = "loadCachedDrugPage_results") {

            .Deprecated()

            if (exists(output.var, envir = globalenv())) {
                    answer <- readline(prompt = paste0("output.var ", output.var, " already exists. Continue? [Y/n] "))
                    if (answer == "n") {
                            rm(list = output.var, envir = globalenv())
                            starting_index <- 1
                    } else {
                            input <- get(output.var, envir = globalenv())
                            starting_index <- length(input)
                    }
            } else {
                    starting_index <- 1
            }

            df <-
                    df %>%
                    dplyr::rename_all(tolower)

            output <- list()

            for (i in 1:nrow(df)) {

                    secretary::typewrite(paste0("[", Sys.time(), "]"), "\t", i, " of ", nrow(df))



                    drug_link <- df$drug_def_link[i]
                    drug_name <- df$drug[i]


                    output[[i]] <- loadCachedScrape(url = drug_link) %>%
                                        dplyr::bind_rows()

                    names(output)[i] <- drug_name

                    #
                    assign(x = output.var,
                           value = output,
                           envir = globalenv())
            }

            secretary::typewrite_bold("Completed.")
    }

