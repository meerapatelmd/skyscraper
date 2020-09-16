library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(rubix)


conn <- chariot::connectAthena()

logged_count <-
        pg13::readTable(conn = conn,
                        schema = "cancergov",
                        tableName = "concept_log") %>%
        dplyr::mutate(concept_timestamp = as.POSIXct(concept_timestamp)) %>%
        dplyr::arrange(desc(concept_timestamp)) %>%
        rubix::filter_first_row() %>%
        dplyr::select(concept_count) %>%
        unlist() %>%
        as.integer()


new_count <- skyscraper::nciDrugCount()



if (new_count > logged_count) {

        pg13::appendTable(conn = conn,
                          schema = "cancergov",
                          tableName = "concept_log",
                          data.frame(concept_timestamp = as.character(Sys.time()),
                                     concept_count = new_count))

        nciDD <- skyscraper::scrapeDictionary(max_page = 39)

        stopifnot(nrow(nciDD) == new_count)

        # With new concept count, the cache from the previous drug scraping should be cleared
        skyscraper::clearSkyscraperCache()

        pg13::writeTable(conn = conn,
                         schema = "cancergov",
                         tableName = "drug_dictionary",
                         nciDD)

        nciDD_concepts <-
                pg13::query(conn = conn,
                            sql_statement = "SELECT cancergov.drug_dictionary.*, cancergov.concept.concept_id
                                FROM cancergov.drug_dictionary
                                LEFT JOIN cancergov.concept
                                    ON LOWER(cancergov.concept.concept_name) = LOWER(cancergov.drug_dictionary.drug);") %>%
                tibble::as_tibble() %>%
                filter(is.na(concept_id))

        concept_table <-
                nciDD_concepts %>%
                dplyr::select(drug) %>%
                dplyr::distinct() %>%
                dplyr::transmute(concept_id = NA,
                                 concept_name = drug,
                                 domain_id = "Drug",
                                 vocabulary_id = "NCI Drug Dictionary",
                                 concept_class_id = "Label",
                                 standard_concept = NA,
                                 concept_code = NA,
                                 valid_start_date = Sys.Date(),
                                 valid_end_date = as.Date("2099-12-31"),
                                 invalid_reason = NA)

        concept_table$concept_id <-
                rubix::make_identifier()+1:nrow(concept_table)


        pg13::appendTable(conn = conn,
                         schema = "cancergov",
                         tableName = "concept",
                         concept_table)

        definition_to_id <-
                pg13::query(conn = conn,
                            sql_statement = "SELECT cancergov.drug_dictionary.*, cancergov.concept.concept_id
                                FROM cancergov.drug_dictionary
                                LEFT JOIN cancergov.concept
                                    ON cancergov.concept.concept_name = cancergov.drug_dictionary.drug;") %>%
                tibble::as_tibble() %>%
                dplyr::distinct()

        nrow(definition_to_id)
        all(!is.na(definition_to_id$concept_id))


        concept_definition_table <-
                definition_to_id %>%
                dplyr::select(concept_id,
                              concept_name = drug,
                              concept_definition = definition) %>%
                dplyr::distinct()

        nrow(concept_definition_table)
        length(unique(concept_definition_table$concept_id))


        concept_definition_table2 <-
                concept_definition_table %>%
                rubix::filter_at_grepl(concept_definition,
                                       grepl_phrase = "Other name") %>%
                mutate(main_concept_name = tolower(stringr::str_remove_all(concept_definition, "[(]{1}.*?[:]{1} |[)]{1}"))) %>%
                dplyr::left_join(concept_table %>%
                                         dplyr::mutate(lower_concept_name = tolower(concept_name)),
                                 by = c("main_concept_name" = "lower_concept_name"),
                                 suffix = c(".synonym", ".main")) %>%
                dplyr::transmute(concept_id = coalesce(concept_id.main, concept_id.synonym),
                                 concept_name = concept_name.synonym) %>%
                dplyr::distinct()

        all(!is.na(concept_definition_table2$concept_id))

        concept_definition_table3 <-
                dplyr::left_join(concept_definition_table,
                                 concept_definition_table2,
                                 by = "concept_name",
                                 suffix = c(".first", ".update")) %>%
                dplyr::distinct() %>%
                dplyr::transmute(concept_id = coalesce(concept_id.update, concept_id.first),
                                 concept_name,
                                 concept_definition) %>%
                distinct()

        pg13::dropTable(conn = conn,
                        schema = "cancergov",
                        tableName = "concept_definition")
        pg13::writeTable(conn = conn,
                         schema = "cancergov",
                         tableName = "concept_definition",
                         concept_definition_table3)
        pg13::dropTable(conn = conn,
                        schema = "cancergov",
                        tableName = "drug_dictionary")


        drugCount <- new_count
        drugLinks <- skyscraper::nciDrugDetailLinks(max_page = 39)
        stopifnot(nrow(drugLinks) == drugCount)


        skyscraper::scrapeDrugPage(df = drugLinks,
                                   progress_bar = F)
        skyscraper::loadCachedDrugPage(df = drugLinks)

        stopifnot(length(loadCachedDrugPage_results)==drugCount)
        failed_to_scrape <-
                loadCachedDrugPage_results %>%
                purrr::keep(function(x) any(is.na(x$X1)))

        not_scraped <-
                loadCachedDrugPage_results %>%
                purrr::keep(function(x) is.null(x))

        drugData <-
                loadCachedDrugPage_results %>%
                dplyr::bind_rows(.id = "Label") %>%
                dplyr::rename(relationship = X1,
                              Label2 = X2) %>%
                dplyr::mutate(relationship = stringr::str_remove_all(relationship, pattern = "[[:punct:]]{1,}$")) %>%
                dplyr::filter_at(vars(!Label),
                                 all_vars(!is.na(.))) %>%
                dplyr::filter(relationship != "Chemical structure") %>%
                dplyr::filter(Label != Label2)

        concept_table <- pg13::readTable(conn = conn,
                                         schema = "cancergov",
                                         tableName = "concept")

        any(is.na(concept_table$concept_name))

        drugData2a2 <-
                drugData %>%
                dplyr::left_join(concept_table,
                                 by = c("Label" = "concept_name")) %>%
                dplyr::distinct()

        any(is.na(drugData2a2$concept_id))

        concept_synonym_table <-
                dplyr::bind_rows(drugData2a2 %>%
                                         dplyr::select(concept_id,
                                                       concept_synonym_name = Label),
                                 drugData2a2 %>%
                                         dplyr::select(concept_id,
                                                       concept_synonym_name = Label2)) %>%
                dplyr::distinct() %>%
                dplyr::mutate(language_concept_id = 4180186)




}


chariot::dcAthena(conn = conn)
