% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/nci.R
\name{nci_run}
\alias{nci_run}
\title{Run NCI Drug Dictionary}
\usage{
nci_run(
  conn,
  conn_fun,
  steps = c("nci_log_count", "get_nci_dd", "get_ncit"),
  crawl_delay = 5,
  size = 10000,
  expiration_days = 10,
  verbose = TRUE,
  render_sql = TRUE
)
}
\description{
Run NCI Drug Dictionary
}
\seealso{
\code{\link[rlang]{parse_expr}}
\code{\link[pg13]{dc}},\code{\link[pg13]{appendTable}}
}
