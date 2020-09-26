#' @title
#' Concept
#' @details
#' '/content/current/CUI/C0009044'path Retrieves CUI and returns a JSON Object classType of Concept
#' '/content/current/CUI/C0009044/atoms'path Retrieve atoms in a CUI and returns a JSON Object classType of Atom
#' '/content/current/CUI/C0009044/definitions'path Retrieve CUI definitions and returns a JSON Object classType of Definition
#' '/content/current/CUI/C0009044/relations'path Retrieve CUI relations and returns a JSON Object classType of ConceptRelation


lookup_concept <-
        function() {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/CUI/C0009044, ## /content/current/CUI/C0009044/atoms, ## /content/current/CUI/C0009044/definitions, ## /content/current/CUI/C0009044/relations
                                                   query = list(ticket = get_service_ticket())
                )
        }


#' @title
#' Atoms
#'
#' @param sabs			 (optional)  Comma-separated list of source vocabularies to include in your search
#' @param ttys			 (optional)  One or more term types
#' @param language	         (optional)  Retrieve only atoms that have a specific language
#' @param includeObsolete	 (optional)  Include content that is obsolete according to the content provider or NLM.
#' @param includeSuppressible	 (optional)  Include content that is suppressible according to NLM Editors.
#' @param pageNumber	         (optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize	         (optional)  Whole number that specifies the number of results to include per page.
#'
#' @details
#' '/content/current/CUI/C0155502/atoms'path Retrieves all atoms for C0155502 and returns a JSON Object classType of Atom
#' '/content/current/CUI/C0155502/atoms/preferred'path Retrieves the default preferred atom of C0155502 and returns a JSON Object classType of Atom
#' '/content/current/CUI/C0155502/atoms?language=ENG'path Retrieves all English language atoms for C0155502 and returns a JSON Object classType of Atom
#' '/content/current/CUI/C0155502/atoms?sabs=SNOMEDCT_US,ICD9CM&ttys=PT'path Retrieve SNOMEDCT_US and ICD9CM preferred terms in C0155502 and returns a JSON Object classType of Atom
#' '/content/current/source/SNOMEDCT_US/111541001/atoms'path Retrieve atoms belonging to SNOMED CT concept 111541001 and returns a JSON Object classType of Atom
#' '/content/current/source/SNOMEDCT_US/111541001/atoms/preferred'path Retrieve the default preferred atom belonging to SNOMED CT concept 111541001 and returns a JSON Object classType of Atom
#' '/content/current/AUI/A8345234'path Retrieve information about AUI A8345234 and returns a JSON Object classType of Atom
#' '/content/current/AUI/A8345234/ancestors'path Retrieve ancestors of a UMLS atom and returns a JSON Object classType of Atom
#' '/content/current/AUI/A8345234/descendants'path Retrieve descendants of a UMLS atom and returns a JSON Object classType of Atom
#' '/content/current/AUI/A8345234/parents'path Retrieve parents of a UMLS atom and returns a JSON Object classType of Atom
#' '/content/current/AUI/A8345234/children'path Retrieve children of a UMLS atom and returns a JSON Object classType of Atom



lookup_atoms <-
        function(
                sabs = NULL,
                ttys = NULL,
                language = NULL,
                includeObsolete = NULL,
                includeSuppressible = NULL,
                pageNumber = NULL,
                pageSize = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/CUI/C0155502/atoms, ## /content/current/CUI/C0155502/atoms/preferred, ## /content/current/CUI/C0155502/atoms?language=ENG, ## /content/current/CUI/C0155502/atoms?sabs=SNOMEDCT_US,ICD9CM&ttys=PT, ## /content/current/source/SNOMEDCT_US/111541001/atoms, ## /content/current/source/SNOMEDCT_US/111541001/atoms/preferred, ## /content/current/AUI/A8345234, ## /content/current/AUI/A8345234/ancestors, ## /content/current/AUI/A8345234/descendants, ## /content/current/AUI/A8345234/parents, ## /content/current/AUI/A8345234/children
                                                   query = list(ticket = get_service_ticket(),
                                                                sabs = sabs,
                                                                ttys = ttys,
                                                                language = language,
                                                                includeObsolete = includeObsolete,
                                                                includeSuppressible = includeSuppressible,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize)
                )
        }

#' @title Definitions
#' @param ticket		 A single-use service ticket is required for each call to the API. See authentication for more information
#' @param sabs			 (optional)  Comma-separated list of source vocabularies to include in your search
#' @param pageNumber		 (optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize		 (optional)  Whole number that specifies the number of results to include per page.
#' @details
#' '/content/current/CUI/C0155502/definitions'path Retrieves definitions of the CUI and returns a JSON Object classType of Definition


lookup_definitions <-
        function(
                sabs = NULL,
                pageNumber = NULL,
                pageSize = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/CUI/C0155502/definitions
                                                   query = list(ticket = get_service_ticket(),
                                                                sabs = sabs,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize)
                )
        }

#' @title Relations
#' @param ticket			A single-use service ticket is required for each call to the API. See authentication for more information
#' @param pageNumber			(optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize			(optional)  Whole number that specifies the number of results to include per page.
#' @details
'/content/current/CUI/C0009044/relations'path Retrieves NLM-asserted relationships of the CUI and returns a JSON Object classType of ConceptRelation



lookup_relations <-
        function(
                ticket,
                pageNumber = NULL,
                pageSize = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/CUI/C0009044/relations
                                                   query = list(ticket = ticket,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize)
                )
        }
#' @title Source Asserted Identifiers
#' @param ticket			A single-use service ticket is required for each call to the API. See authentication for more information
#' @details
'/content/current/source/SNOMEDCT_US/9468002'path Retrieves Source Concept and returns a JSON Object classType of SourceAtomCluster
'/content/current/source/MSH/D015242'path Retrieves Source Descriptor and returns a JSON Object classType of SourceAtomCluster
'/content/current/source/LNC/54112-8'path Retrieves Code and returns a JSON Object classType of SourceAtomCluster
'/content/current/source/SNOMEDCT_US/9468002/atoms'path Retrieve atoms in a source-asserted identifier and returns a JSON Object classType of Atom
'/content/current/source/SNOMEDCT_US/9468002/parents'path Retrieve immediate parents of a source-asserted identifier and returns a JSON Object classType of SourceAtomCluster
'/content/current/source/SNOMEDCT_US/9468002/children'path Retrieve immediate children of source-asserted identifier and returns a JSON Object classType of SourceAtomCluster
'/content/current/source/SNOMEDCT_US/9468002/ancestors'path Retrieve all ancestors of a source-asserted identifier and returns a JSON Object classType of SourceAtomCluster
'/content/current/source/SNOMEDCT_US/9468002/descendants'path Retrieve all descendants of source-asserted identifier and returns a JSON Object classType of SourceAtomCluster
'/content/current/source/SNOMEDCT_US/9468002/attributes'path Retrieves information about source-asserted attributes and returns a JSON Object classType of Attribute



lookup_source_asserted_identifiers <-
        function(
                ticket) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/source/SNOMEDCT_US/9468002, ## /content/current/source/MSH/D015242, ## /content/current/source/LNC/54112-8, ## /content/current/source/SNOMEDCT_US/9468002/atoms, ## /content/current/source/SNOMEDCT_US/9468002/parents, ## /content/current/source/SNOMEDCT_US/9468002/children, ## /content/current/source/SNOMEDCT_US/9468002/ancestors, ## /content/current/source/SNOMEDCT_US/9468002/descendants, ## /content/current/source/SNOMEDCT_US/9468002/attributes
                                                   query = list(ticket = ticket)
                )
        }
#' @title Parents And Children
#' @param ticket			A single-use service ticket is required for each call to the API. See authentication for more information
#' @param pageNumber			(optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize			(optional)  Whole number that specifies the number of results to include per page.
#' @details
'/content/current/source/SNOMEDCT_US/9468002/parents'path Retrieves parents a source-asserted identifier and returns a JSON Object classType of SourceAtomCluster*
        '/content/current/source/SNOMEDCT_US/9468002/children'path Retrieves children of a source-asserted identifier and returns a JSON Object classType of SourceAtomCluster*



        lookup_parents_and_children <-
        function(
                ticket,
                pageNumber = NULL,
                pageSize = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/source/SNOMEDCT_US/9468002/parents, ## /content/current/source/SNOMEDCT_US/9468002/children
                                                   query = list(ticket = ticket,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize)
                )
        }
#' @title Ancestors And Descendants
#' @param ticket			A single-use service ticket is required for each call to the API. See authentication for more information
#' @param pageNumber			(optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize			(optional)  Whole number that specifies the number of results to include per page.
#' @details
'/content/current/source/SNOMEDCT_US/9468002/ancestors'path Retrieves ancestors of a source-asserted identifier and returns a JSON Object classType of SourceAtomCluster*
        '/content/current/source/SNOMEDCT_US/9468002/descendants'path Retrieves descendants of a source-asserted identifier and returns a JSON Object classType of SourceAtomCluster*



        lookup_ancestors_and_descendants <-
        function(
                ticket,
                pageNumber = NULL,
                pageSize = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/source/SNOMEDCT_US/9468002/ancestors, ## /content/current/source/SNOMEDCT_US/9468002/descendants
                                                   query = list(ticket = ticket,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize)
                )
        }
#' @title Relations
#' @param ticket			A single-use service ticket is required for each call to the API. See authentication for more information
#' @param includeRelationLabels			(optional)  One or more relation labels
#' @param includeAdditionalRelationLabels			(optional)  One or more relation attribute
#' @param includeObsolete			(optional)  Include content that is obsolete according to the content provider or NLM.
#' @param includeSuppressible			(optional)  Include content that is suppressible according to NLM Editors.
#' @param pageNumber			(optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize			(optional)  Whole number that specifies the number of results to include per page.
#' @details
'/content/current/source/LNC/44255-8/relations'path Retrieves relationships of LOINC code 44255-8 and returns a JSON Object classType of AtomClusterRelation



lookup_relations <-
        function(
                ticket,
                includeRelationLabels = NULL,
                includeAdditionalRelationLabels = NULL,
                includeObsolete = NULL,
                includeSuppressible = NULL,
                pageNumber = NULL,
                pageSize = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/source/LNC/44255-8/relations
                                                   query = list(ticket = ticket,
                                                                includeRelationLabels = includeRelationLabels,
                                                                includeAdditionalRelationLabels = includeAdditionalRelationLabels,
                                                                includeObsolete = includeObsolete,
                                                                includeSuppressible = includeSuppressible,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize)
                )
        }
#' @title Subsets
#' @param ticket			A single-use service ticket is required for each call to the API. See authentication for more information
#' @param pageNumber			(optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize			(optional)  Whole number that specifies the number of results to include per page.
#' @param language			(optional)  3-letter abbreviation for language
#' @details
'/subsets/current'path Retrieves information about all subsets from the current release and returns a JSON Object classType of Subset
'/subsets/current/source/SNOMEDCT_US/6011000124106'path Retrieves information for a SNOMED CT subset and returns a JSON Object classType of Subset
'/subsets/current/source/SNOMEDCT_US/6011000124106/members'path Retrieves members of a SNOMED CT subset and returns a JSON Object classType of SourceConceptSubsetMember
'/subsets/current/source/SNOMEDCT_US/6011000124106/member/89361000119103'path Retrieves an individual member of a SNOMED CT subset and returns a JSON Object classType of SourceConceptSubsetMember



lookup_subsets <-
        function(
                ticket,
                pageNumber = NULL,
                pageSize = NULL,
                language = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /subsets/current, ## /subsets/current/source/SNOMEDCT_US/6011000124106, ## /subsets/current/source/SNOMEDCT_US/6011000124106/members, ## /subsets/current/source/SNOMEDCT_US/6011000124106/member/89361000119103
                                                   query = list(ticket = ticket,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize,
                                                                language = language)
                )
        }
#' @title Attributes
#' @param ticket			A single-use service ticket is required for each call to the API. See authentication for more information
#' @param pageNumber			(optional)  Whole number that specifies which page of results to fetch.
#' @param pageSize			(optional)  Whole number that specifies the number of results to include per page.
#' @param includeAttributeNames			(optional)  One or more attribute names
#' @details
'/content/current/source/SNOMEDCT_US/9468002/attributes'path Retrieves attributes of the SNOMED CT concept and returns a JSON Object classType of Attribute



lookup_attributes <-
        function(
                ticket,
                pageNumber = NULL,
                pageSize = NULL,
                includeAttributeNames = NULL) {


                link_response <- httr::GET(url = baseURL,
                                           path = ## /content/current/source/SNOMEDCT_US/9468002/attributes
                                                   query = list(ticket = ticket,
                                                                pageNumber = pageNumber,
                                                                pageSize = pageSize,
                                                                includeAttributeNames = includeAttributeNames)
                )
        }






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
        function(CUI = NULL,

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
