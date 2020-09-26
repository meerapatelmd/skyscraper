hemonc_concepts <-
        chariot::queryAthena("WITH hemonc AS (
                                     SELECT DISTINCT
                                     cs.concept_id, cs.concept_synonym_name
                                     FROM concept c
                                     LEFT JOIN concept_synonym cs
                                     ON cs.concept_id = c.concept_id
                                     WHERE c.vocabulary_id = 'HemOnc'
                                                AND c.invalid_reason IS NULL
                                                AND c.domain_id = 'Drug'
                             )

                             SELECT *
                             FROM hemonc ho
                             LEFT JOIN nci_evs.mrconso mth
                             ON LOWER(mth.str) = LOWER(ho.concept_synonym_name);")





schema = "omop_drug_to_umls_api"
tableName <- "umls_mth_results"

library(tidyverse)
library(chariot)

conn <- chariot::connectAthena()
hemonc_concepts <-
        chariot::queryAthena("SELECT DISTINCT
                             cs.concept_synonym_name
                             FROM concept c
                             LEFT JOIN concept_synonym cs
                             ON cs.concept_id = c.concept_id
                             WHERE c.vocabulary_id = 'HemOnc'
                                        AND c.invalid_reason IS NULL
                                        AND c.domain_id = 'Drug';") %>%
        unlist()


hemonc_concepts <- sample(hemonc_concepts)
while (length(hemonc_concepts)) {
        hemonc_concept <- hemonc_concepts[1]

        pg13::query(conn = conn,
        pg13::buildQueryString(schema = "nci_evs",
                               tableName = "mrconso",
                               whereLikeField = "str",
                               string = hemonc_concept,
                               split = " |[[:punct:]]")
        )


        hemonc_concepts <- hemonc_concepts[-1]
}
