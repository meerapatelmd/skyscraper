#' @title
#' Update the ChemiDPlus Schema with New Data
#' @description
#' If the current installation of the chemidplusData Package is not up-to-date, it is installed and the current `chemidplus` schema is dropped and a new schema is written with the fresh data.
#' @seealso
#'  \code{\link[utils]{capture.output}}
#'  \code{\link[devtools]{remote-reexports}}
#'  \code{\link[pg13]{dropSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{writeTable}}
#'  \code{\link[rubix]{map_names_set}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{map2}}
#'  \code{\link[dplyr]{mutate_all}}
#'  \code{\link[lubridate]{ymd_hms}}
#' @rdname updateChemiDPlus
#' @export
#' @importFrom utils capture.output
#' @importFrom devtools install_github
#' @importFrom pg13 dropSchema createSchema writeTable
#' @importFrom rubix map_names_set
#' @importFrom purrr map map2
#' @importFrom dplyr mutate_at
#' @importFrom lubridate ymd_hms
#' @importFrom magrittr %>%

updateChemiDPlus <-
        function(conn) {

                install_msg <- utils::capture.output(devtools::install_github("meerapatelmd/chemidplusData"), type = "message")

                if (!any(grepl(pattern = "the SHA1.*has not changed since last install",
                          x = install_msg))) {


                        pg13::dropSchema(conn = conn,
                                         schema = "chemidplus",
                                         cascade = TRUE)

                        pg13::createSchema(conn = conn,
                                           schema = "chemidplus")

                        require(chemidplusData)
                        DATASETS <- data(package = "chemidplusData")
                        DATASETS <- DATASETS$results[, "Item"]


                        DATA <-
                                DATASETS %>%
                                rubix::map_names_set(get) %>%
                                purrr::map(function(x) x %>%
                                           dplyr::mutate_at(vars(ends_with("datetime")),
                                                            lubridate::ymd_hms))

                        DATA %>%
                                purrr::map2(names(DATA),
                                            function(x,y) pg13::writeTable(conn = conn,
                                                                           schema = "chemidplus",
                                                                           tableName = y,
                                                                           x))

                }
        }


