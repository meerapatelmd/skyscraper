library(tidyverse)
library(secretary)
library(chariot)
library(pg13)
library(skyscraper)
library(centipede)


input <-
chariot::queryAthena("SELECT *
                     FROM chemidplus.rn_url_validity
                     WHERE is_404 = 'TRUE';",
                     override_cache = TRUE)

if (nrow(input)) {
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
               "WITH invalid_urls AS (
            	SELECT DISTINCT rnuv.rn_url AS invalid_url
            	FROM chemidplus.rn_url_validity rnuv
            	WHERE rnuv.is_404 = 'TRUE'
            ),
            invalid_concepts AS (
            	SELECT DISTINCT raw_concept AS invalid_concept
            	FROM chemidplus.old_registry_number_log rnl
            	WHERE rnl.rn_url IN (
            	   SELECT invalid_url
            	   FROM invalid_urls
            	)
            )


            INSERT INTO chemidplus.registry_number_log
            SELECT *
            FROM chemidplus.old_registry_number_log rnl
            WHERE rnl.raw_concept NOT IN (
            	SELECT invalid_concept
            	FROM invalid_concepts
            )
            ;")

    pg13::renameTable(conn = conn,
                      schema = "chemidplus",
                      "rn_url_validity",
                      "old_rn_url_validity")

    pg13::send(conn = conn,
               sql_statement = "CREATE TABLE chemidplus.rn_url_validity (
        rnuv_datetime timestamp without time zone,
        rn_url character varying(255),
        is_404 character varying(255)
    );")

    pg13::send(conn = conn,
               sql_statement =
                    "
                    INSERT INTO chemidplus.rn_url_validity
                    SELECT *
                    FROM chemidplus.old_rn_url_validity
                    WHERE is_404 = 'FALSE';
                    ")

    pg13::dropTable(conn = conn,
                    schema = "chemidplus",
                    tableName = "old_rn_url_validity")


    pg13::dropTable(conn = conn,
                    schema = "chemidplus",
                    tableName = "old_registry_number_log")

    chariot::dcAthena(conn = conn)

    skyscraper::export_schema_to_data_repo(target_dir = "~/GitHub/chemidplusData/",
                                               schema = "chemidplus")
}

