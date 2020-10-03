

library(tidyverse)
library(chariot)

conn <- chariot::connectAthena()
all_hemonc_names <-
        chariot::queryAthena("SELECT DISTINCT
                             cs.concept_id, cs.concept_synonym_name
                             FROM concept c
                             LEFT JOIN concept_synonym cs
                             ON cs.concept_id = c.concept_id
                             WHERE c.vocabulary_id = 'HemOnc'
                                        AND c.invalid_reason IS NULL
                                        AND c.domain_id = 'Drug';")

input_list <-
        split(all_hemonc_names, all_hemonc_names$concept_id)

randomized_list <- input_list[sample(names(input_list))] %>%
                        purrr::map(function(x) x %>%
                                                dplyr::select(concept_synonym_name) %>%
                                                unlist() %>%
                                                unname())

output <- list()
for (i in 1:length(randomized_list)) {

        output[[i]] <- list()
        names(output)[i] <- names(randomized_list)[i]
        for (j in 1:length(randomized_list[[i]])) {

                        concept <- randomized_list[[i]][j]

                        output[[i]][[j]] <-
                                CallMeMaybe::umls_api_search(string = concept)

                        Sys.sleep(sample(3:10,1))

        }
         results <- dplyr::bind_rows(output[[i]])

         if ("rootSource" %in% colnames(results)) {

                 results <-
                         results %>%
                         dplyr::transmute(
                                 search_datetime = Sys.time(),
                                 concept_id = as.integer(names(randomized_list)[i]),
                                 string = concept,
                                 searchType,
                                 classType,
                                 pageSize,
                                 pageNumber,
                                 ui,
                                 ui_str = name,
                                 rootSource
                         )


         } else {
                 results <-
                         results %>%
                         dplyr::transmute(
                                 search_datetime = Sys.time(),
                                 concept_id = as.integer(names(randomized_list)[i]),
                                 string = concept,
                                 searchType,
                                 classType,
                                 pageSize,
                                 pageNumber,
                                 ui,
                                 ui_str = name,
                                 rootSource = NA
                         )
         }

                Tables <- pg13::lsTables(conn = conn,
                                         schema = "omop_drug_to_umls_api")

                if (!("SEARCH_LOG" %in% Tables)) {

                        pg13::send(conn = conn,
                                   sql_statement =
                                           "CREATE TABLE omop_drug_to_umls_api.search_log (
                                                        search_datetime timestamp without time zone,
                                                        concept_id integer,
                                                        concept character varying(255),
                                                        string character varying(255),
                                                        searchtype character varying(255),
                                                        classtype character varying(255),
                                                        pagesize integer,
                                                        pagenumber integer,
                                                        ui character varying(255),
                                                        ui_str character varying(255),
                                                        rootsource character varying(255)
                                                )
                                                ;
                                                ")

                        pg13::appendTable(conn = conn,
                                          schema = "omop_drug_to_umls_api",
                                          tableName = "SEARCH_LOG",
                                          results)
                } else {
                        pg13::appendTable(conn = conn,
                                          schema = "omop_drug_to_umls_api",
                                          tableName = "SEARCH_LOG",
                                          results)
                }

}
