% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pm.R
\name{pm_run}
\alias{pm_run}
\title{Run the complete PubMed Scrape}
\usage{
pm_run(conn, conn_fun, search_term, verbose = TRUE, render_sql = TRUE)
}
\description{
Scrape and the store the earliest and latest PubMed Publications for a search term by executing \code{\link{get_pm_earliest}} in concert with \code{\link{get_pm_latest}}. This is followed by an indiscriminate union between PM_EARLIEST and PM_LATEST Tables and refreshed as the PM_RESULTS Table.
}
