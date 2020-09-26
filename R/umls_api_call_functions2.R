#' @title
#' Semantic Network
#' @details
#' /semantic-network/current/TUI/T109'path Retrieves TUI and returns a JSON Object classType of SemanticType



lookup_semantic_network <-
        function() {

                link_response <- httr::GET(url = baseURL,
                                           path = ## /semantic-network/current/TUI/T109
                                                   query = list(ticket = get_service_ticket())
                )
        }

#' @title
#' Lookup Content Views
#'
#' @param pageNumber    (optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize      (optional)  Whole number that specifies the number of results to include per page.
#' @details
#' '/content-views/current'path Retrieves information about all subsets from the current release and returns a JSON Object classType of ContentView
#' '/content-views/current/CUI/C2711988'path Retrieves information for the SNOMED CT CORE Problem List content view and returns a JSON Object classType of ContentView
#' '/content-views/current/CUI/C2711988/members'path Retrieves members of the SNOMED CT CORE Problem List content view and returns a JSON Object classType of SourceConceptContentViewMember*
#' '/content-views/current/CUI/C2711988/member/238788004'path Retrieves an individual member of the SNOMED CT CORE Problem List content view and returns a JSON Object classType of SourceConceptContentViewMember


lookup_content_views <-
        function(
                pageNumber = NULL,
                pageSize = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content-views/current, ## /content-views/current/CUI/C2711988, ## /content-views/current/CUI/C2711988/members, ## /content-views/current/CUI/C2711988/member/238788004
                                                   query = list(ticket = get_service_ticket(),
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize)
                )
        }

#' @title
#' Lookup a Crosswalk
#'
#' @param targetSource			 (optional)  Returns codes from the specified UMLS vocabulary
#' @param includeObsolete		 (optional)  Determines whether to return obsolete codes.
#' @param pageNumber			 (optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize			 (optional)  Whole number that specifies the number of results to include per page.
#' @details
#' '/crosswalk/current/source/HPO/HP:0001947'path Retrieves all codes that share a UMLS CUI with HP:0001947 and returns a JSON Object classType of SourceAtomCluster
#' '/crosswalk/current/source/HPO/HP:0001947?targetSource=SNOMEDCT_US'path Retrieves all SNOMEDCT_US codes that share a UMLS CUI with HP:0001947 and returns a JSON Object classType of SourceAtomCluster



lookup_crosswalk <-
        function(
                targetSource = NULL,
                includeObsolete = NULL,
                pageNumber = NULL,
                pageSize = NULL) {

                baseURL <- "https://uts-ws.nlm.nih.gov/rest"

                link_response <- httr::GET(url = baseURL,
                                           path = ## /crosswalk/current/source/HPO/HP:0001947, ## /crosswalk/current/source/HPO/HP:0001947?targetSource=SNOMEDCT_US
                                                   query = list(ticket = get_service_ticket(),
                                                                targetSource = targetSource,
                                                                includeObsolete = includeObsolete,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize)
                )
        }
