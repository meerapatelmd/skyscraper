#' @title
#' Setup the ChemiDPlus Schema
#' @description
#' If a `chemidplus` schema does not exist, a `chemidplus` Schema at a given Postgres connection with the data found in the chemidplusData Package. If the `chemidplus` Schema needs to be updated with new data added to the chemidplusData Package, use the updateChemiDPlus function instead.
#' @param conn Postgres connection
#' @details
#' The chemidplusData Package is force installed first. if a `chemidplus` schema is not present in the given connection,`chemidplus` schema is created, and the data exported in the chemidplusData R Package is written as tables.
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{createSchema}},\code{\link[pg13]{writeTable}}
#'  \code{\link[devtools]{remote-reexports}}
#'  \code{\link[rubix]{map_names_set}}
#'  \code{\link[purrr]{map}},\code{\link[purrr]{map2}}
#'  \code{\link[dplyr]{mutate_all}}
#'  \code{\link[lubridate]{ymd_hms}}
#' @rdname setupChemiDPlus
#' @export
#' @importFrom pg13 lsSchema createSchema writeTable
#' @importFrom devtools install_github
#' @importFrom rubix map_names_set
#' @importFrom purrr map map2
#' @importFrom dplyr mutate_at
#' @importFrom lubridate ymd_hms
#' @importFrom magrittr %>%

setupChemiDPlus <-
        function(conn) {

                #conn <- chariot::connectAthena()

                devtools::install_github("meerapatelmd/chemidplusData",
                                         force = TRUE,
                                         quiet = TRUE)


                pg_schemas <- pg13::lsSchema(conn = conn)
                if (!("chemidplus" %in% pg_schemas)) {


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

