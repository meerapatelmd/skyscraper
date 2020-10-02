#' @title
#' Map a skyscraper Schema to a Data Package Repository
#'
#' @description
#' Each Postgres schema governed by this package has a corresponding Data Package, where updates to the Postgres schema are exported as csvs, packaged, and pushed to a GitHub repo.
#'
#' @param repo_username         Github username to which the Data Package belongs, Default: 'meerapatelmd'
#' @seealso
#'  \code{\link[tibble]{tribble}}
#'  \code{\link[dplyr]{mutate}}
#' @rdname map_schema
#' @family local maintenance
#' @export
#' @importFrom tibble tribble
#' @importFrom dplyr mutate
#' @importFrom magrittr %>%


map_schema <-
        function(repo_username = "meerapatelmd") {


                tibble::tribble(~schema, ~dataPackage, ~tables,
                                "cancergov", "cancergovData", "c('DRUG_DICTIONARY', 'DRUG_DICTIONARY_LOG', 'DRUG_LINK', 'DRUG_LINK_SYNONYM', 'DRUG_LINK_URL', 'DRUG_LINK_NCIT')",
                                "chemidplus_search", "chemidplusSearchData", "c('CLASSIFICATION', 'LINKS_TO_RESOURCES', 'NAMES_AND_SYNONYMS', 'REGISTRY_NUMBER_LOG', 'REGISTRY_NUMBERS', 'RN_URL_VALIDITY')",
                                "chemidplus", "chemidplusData", "c('CLASSIFICATION', 'LINKS_TO_RESOURCES', 'NAMES_AND_SYNONYMS', 'REGISTRY_NUMBER_LOG', 'REGISTRY_NUMBERS', 'RN_URL_VALIDITY')",
                                "pubmed_search", "pubmedSearchData", NA) %>%
                        dplyr::mutate(repo = paste0(repo_username, "/", dataPackage))

        }
