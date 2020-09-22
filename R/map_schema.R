#' @title
#' Map a skyscraper Schema to a Data Package Repository
#' @description
#' Each Postgres schema governed by this package has a corresponding Data Package, where updates to the Postgres schema are exported as csvs, packaged, and pushed to a GitHub repo.
#'
#' @param repo_username         Github username to which the Data Package belongs, Default: 'meerapatelmd'
#' @seealso
#'  \code{\link[tibble]{tribble}}
#'  \code{\link[dplyr]{mutate}}
#' @rdname map_schema
#' @export
#' @importFrom tibble tribble
#' @importFrom dplyr mutate
#' @importFrom magrittr %>%


map_schema <-
        function(repo_username = "meerapatelmd") {


                tibble::tribble(~schema, ~dataPackage,
                                "cancergov", "cancergovData",
                                "chemidplus_search", "chemidplusSearchData",
                                "chemidplus", "chemidplusData") %>%
                        dplyr::mutate(repo = paste0(repo_username, "/", dataPackage))

        }
