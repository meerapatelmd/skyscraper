CREATE TABLE IF NOT EXISTS chemidplus.concept (
    concept_id bigint,
    concept_name character varying(255),
    domain_id character varying(255),
    vocabulary_id character varying(255),
    concept_class_id character varying(255),
    standard_concept character varying(255),
    concept_code character varying(255),
    valid_start_date date,
    valid_end_date date,
    invalid_reason character varying(255)
);


CREATE TABLE IF NOT EXISTS chemidplus.concept_ancestor (
    ancestor_concept_id bigint,
    descendant_concept_id bigint,
    min_levels_of_separation bigint,
    max_levels_of_separation bigint
);

CREATE TABLE IF NOT EXISTS chemidplus.concept_relationship (
    concept_id_1 bigint,
    concept_id_2 bigint,
    relationship_id character varying(255),
    valid_start_date date,
    valid_end_date date,
    invalid_reason character varying(255)
);


CREATE TABLE IF NOT EXISTS chemidplus.concept_synonym (
    concept_id bigint,
    concept_synonym_name character varying(255),
    language_concept_id bigint
);
