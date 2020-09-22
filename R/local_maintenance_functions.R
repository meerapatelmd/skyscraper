#' @title
#' Skyscraper Local Data Maintenance Functions
#'
#' @description
#' Local maintenance functions are meant to be used to instantiate or update a local skyscraper schema with the Data Package Repo. This is important for keeping data updated on one machine when there have been additions made to the Remote Data Package Repository.
#'
#' @section
#' Check Schema Status:
#' All the Data Packages in the Schema Map returned by \code{\link{map_schema}} are installed if there are changes to the SHA hash. An "updated" field is added to the Schema Map that contain the values of TRUE if a new installation occurred and FALSE otherwise. If an update to the schema is desired after running this function, \code{\link{update_schemas}}, should be run with `force_update` set to `TRUE`.
#'
#' @section
#' Update Schemas:
#' Checking Schema Status is not a required prerequisite to update skyscraper schemas. However, if the status is checked before running an update, the update needs to be run with force_update set to `TRUE` since the execution of the update occurs on the condition of requiring a new installation, which the \code{\link{schema_status}} would have already executed.
#'
#' @name local_maintenance_functions
NULL
