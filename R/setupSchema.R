#' @title
#' Setup a Schema
#' @description
#' If the schema does not exist, the schema at a given Postgres connection with the data found in the chemidplusData Package. If the schema needs to be updated with new data added to the corresponding data Package, use the updateChemiDPlus function instead.
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
#' @export
#' @importFrom pg13 lsSchema createSchema writeTable
#' @importFrom devtools install_github
#' @importFrom rubix map_names_set filter_for
#' @importFrom purrr map map2
#' @importFrom dplyr mutate_at
#' @importFrom lubridate ymd_hms
#' @importFrom magrittr %>%

setupSchema <-
        function(conn,
                 schema = "chemidplus") {

                #conn <- chariot::connectAthena()
                #

                schema_map <- schemaMap()


                pg_schemas <- pg13::lsSchema(conn = conn)

                if (!(schema %in% pg_schemas)) {

                        dataPackage <- schema_map %>%
                                                rubix::filter_for(filter_col = "schema",
                                                                  inclusion_vector = schema) %>%
                                                dplyr::select(dataPackage) %>%
                                                unname() %>%
                                                unlist()

                        devtools::install_github(paste0("meerapatelmd/", dataPackage),
                                                 force = TRUE,
                                                 quiet = TRUE)


                        pg13::createSchema(conn = conn,
                                           schema = schema)

                        require(dataPackage,
                                character.only = TRUE)

                        DATASETS <- data(package = dataPackage)
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
                                                                           schema = schema,
                                                                           tableName = y,
                                                                           x))

                }

        }

