library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(centipede)


input_a <-
chariot::queryAthena(
"
SELECT DISTINCT raw_concept
        	FROM chemidplus.registry_number_log rnl
        	WHERE rnl.rn_url LIKE 'https://chem.nlm.nih.gov/chemidplus/rn/ %';
",
override_cache = TRUE) %>%
    unlist() %>%
    unname()

input_b <-
chariot::queryAthena(
    "
SELECT DISTINCT *
        	FROM chemidplus.registry_number_log rnl
        	WHERE rnl.rn_url IS NOT NULL;
",
    override_cache = TRUE)

input_b <-
input_b %>%
    rubix::filter_at_grepl(rn_url,
                           grepl_phrase = "https://chem.nlm.nih.gov/chemidplus/rn/[^0-9]{1}") %>%
    dplyr::select(raw_concept) %>%
    dplyr::distinct() %>%
    unlist() %>%
    unname()

input <- c(input_a,
           input_b) %>%
            unique()

if (length(input)) {
conn <- chariot::connectAthena()
pg13::renameTable(conn = conn,
                  schema = "chemidplus",
                  "registry_number_log",
                  "old_registry_number_log")

pg13::send(conn = conn,
           sql_statement = "CREATE TABLE chemidplus.registry_number_log (
                   rnl_datetime timestamp without time zone,
                   raw_concept character varying(255),
                   processed_concept character varying(255),
                   type character varying(255),
                   url character varying(255),
                   response_received character varying(255),
                   no_record character varying(255),
                   response_recorded character varying(255),
                   compound_match character varying(255),
                   rn character varying(255),
                   rn_url character varying(255)
           );")


pg13::send(conn = conn,
           sql_statement =
               paste0(
               "
                INSERT INTO chemidplus.registry_number_log
                SELECT *
                FROM chemidplus.old_registry_number_log rnl
                WHERE rnl.raw_concept NOT IN (",

                    paste(paste0("'", input, "'"), collapse = ","),

                ")
                ;")
           )

pg13::dropTable(conn = conn,
                schema = "chemidplus",
                tableName = "old_registry_number_log")

chariot::dcAthena(conn = conn)


input_a <-
    chariot::queryAthena(
        "
SELECT DISTINCT raw_concept
        	FROM chemidplus.registry_number_log rnl
        	WHERE rnl.rn_url LIKE 'https://chem.nlm.nih.gov/chemidplus/rn/ %';
",
        override_cache = TRUE) %>%
    unlist() %>%
    unname()

input_b <-
    chariot::queryAthena(
        "
SELECT DISTINCT *
        	FROM chemidplus.registry_number_log rnl
        	WHERE rnl.rn_url IS NOT NULL;
",
        override_cache = TRUE)

input_b <-
    input_b %>%
    rubix::filter_at_grepl(rn_url,
                           grepl_phrase = "https://chem.nlm.nih.gov/chemidplus/rn/[^0-9]{1}") %>%
    dplyr::select(raw_concept) %>%
    dplyr::distinct() %>%
    unlist() %>%
    unname()

new_input <- c(input_a,
           input_b) %>%
    unique()


skyscraper::export_schema_to_data_repo(target_dir = "~/GitHub/chemidplusData/",
                                       schema = "chemidplus")
}
