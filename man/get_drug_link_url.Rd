% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cg-internals.R
\name{get_drug_link_url}
\alias{get_drug_link_url}
\title{Get the URLs found in a Drug Link}
\usage{
get_drug_link_url(
  conn,
  drug_link,
  response,
  sleep_time = 5,
  expiration_days = 30,
  verbose = TRUE,
  render_sql = TRUE
)
}
\arguments{
\item{conn}{Postgres connection object.}

\item{response}{(Optional) The response returned when the url supplied as the \code{drug_link} is parsed. If not provided, a new response is received. This is an option to reduce repetitive scrapes of the same URL if multiple sections of the same page are being parsed.}

\item{sleep_time}{Time in seconds for the system to sleep before each scrape with \code{\link[xml2]{read_html}}.}

\item{verbose}{When reading from a slow connection, this prints some
output on every iteration so you know its working.}
}
\description{
Run the full sequence that scrapes, parses, and stores the NCI Drug Dictionary found at CancerGov.org and any correlates to the NCI Thesaurus in a Postgres Database.
}
\section{Drug Detail Links}{

The links to Drug Pages are scraped from the Data Dictionary URL over the maximum page number and are saved to a Drug Link Table in the \code{cancergov} schema. The URLs in the Drug Link Table are then scraped for any HTML Tables of synonyms and the results are written to a Drug Link Synonym Table. The links to active clinical trials and NCIt mappings are also derived and stored in their respective tables.
}

\seealso{
\code{\link[SqlRender]{render}}
\code{\link[pg13]{query}},\code{\link[pg13]{appendTable}}
\code{\link[xml2]{read_xml}}
\code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_text}}
\code{\link[tibble]{tibble}}
}
