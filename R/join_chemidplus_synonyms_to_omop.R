#' @title
#' Join ChemiDPlus Results to the OMOP Vocabulary by Concept
#'
#' @description
#' All the Names and Synonyms for a given Raw Concept are joined in lowercase to the lowercase Concept Synonyms found in the OMOP Concept Synonym Table that are valid and belong to the Drug domain.
#'
#' @param conn                  Postgres connection
#' @param chemidplusSchema      Schema that contains the Registry Number Log and Names and Synonyms` Tables
#' @param vocabularySchema      Schema that contains the OMOP Concept and Concept Synonym Tables, Default: 'public'
#' @param vocabularyIds         Vocabulary Ids in OMOP Concept Table to filter results for. Default: c("HemOnc", "RxNorm", "RxNorm Extension", "ATC")
#'
#' @return
#' Dataframe with all the Raw Concepts from the Registry Number Log with a Registry Number,  the Timestamp of when the Names and Synonyms Table was appended, Registry Number URL, the Synonym Type and Synonym scraped from the URL, and all the fields found in the OMOP Concept Table that the Synonym mapped to via any of the OMOP Concept Synonyms (not returned).
#'
#' @seealso
#'  \code{\link[pg13]{query}}
#'  \code{\link[SqlRender]{render}}
#'
#' @rdname join_chemidplus_omop_synonyms
#' @export
#' @importFrom pg13 query
#' @importFrom SqlRender render




join_chemidplus_omop_synonyms <-
        function(conn,
                 chemidplusSchema,
                 vocabularySchema = "public",
                 vocabularyIds = c('HemOnc', 'RxNorm', 'RxNorm Extension', 'ATC')) {


                #conn <- chariot::connectAthena()
                #
                vocabularyIds <- paste0("'", vocabularyIds, "'")

                pg13::query(conn = conn,
                            sql_statement =
                                    SqlRender::render(
                                                "
                                                SELECT rnl.raw_concept, nas.*, c.*
                                                FROM @chemidplusSchema.registry_number_log rnl
                                                LEFT JOIN @chemidplusSchema.names_and_synonyms nas
                                                ON rnl.rn_url = nas.rn_url
                                                LEFT JOIN @vocabularySchema.concept_synonym cs
                                                ON LOWER(cs.concept_synonym_name) = LOWER(nas.concept_synonym_name)
                                                LEFT JOIN @vocabularySchema.concept c
                                                ON c.concept_id = cs.concept_id
                                                WHERE
                                                        c.invalid_reason IS NULL AND
                                                        c.domain_id = 'Drug' AND
                                                        c.vocabulary_id IN (@vocabularyIds)
                                                        AND rnl.rn_url IS NOT NULL
                                                ;
                                                ",
                                    chemidplusSchema = chemidplusSchema,
                                    vocabularySchema = vocabularySchema,
                                    vocabularyIds = vocabularyIds))

}
