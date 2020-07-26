#' Get Drug Dictionary Data
#' @description Scrape, Load, and Format in the OMOP Vocabulary Concept Table format.
#' @import rubix
#' @import dplyr
#' @export



getDrugDictionaryConcept <-
    function(conn,
             max_page) {

                scrapeCancerGovDict(max_page = max_page)

                .input <- loadScrapedDrugDict(max_page = max_page)

                new_ids <- rubix::make_identifier()

                .input %>%
                    dplyr::mutate(concept_id = new_ids+(1:nrow(.input))) %>%
                    dplyr::select(page = Page,
                                  concept_id,
                                  concept_name = DRUG,
                                  concept_definition = DEFINITION) %>%
                    dplyr::mutate(domain_id = "Drug") %>%
                    dplyr::mutate(concept_class_id = "Drug") %>%
                    dplyr::mutate(vocabulary_id = "CancerGov") %>%
                    dplyr::mutate(page_url = paste0("https://www.cancer.gov/publications/dictionaries/cancer-drug?expand=ALL&page=", page))

    }
