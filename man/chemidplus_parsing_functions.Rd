% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/cdp-internals.R
\name{chemidplus_parsing_functions}
\alias{chemidplus_parsing_functions}
\title{ChemiDPlus Parsing Functions}
\arguments{
\item{response}{"xml_document" "xml_node" class object returned by calling xml2::read_html on the rn_url}
}
\description{
There are 2 HTML responses involved in ChemiDPlus scraping. The first HTML response is associated with the concept search while the second is for the 1 to 5 Registry Numbers and their URLs if records were found. The ChemiDPlus parsing functions delineate the response into 3 groups: searches the resulted in no records, a single record, or multiple records (currently limited to parsing a maximum of 5 records).
}
\section{No Records}{

If the ChemiDPlus search url response contained the phrase "The following query produced no records:", it is marked as not having any records.
}

\section{Single Hit}{

If a ChemiDPlus search results in a single match, the URL returned is the RN URL itself that is derived from the "h1" HTML node. For multiple matches, the "h1" HTML node returns blank.
}

\section{Multiple Hits}{

If a ChemiDPlus search has multiple possible matches, the landing page where the first 5 matches are listed is scraped for each of their RNs.
}

