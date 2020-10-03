#' @title FUNCTION_TITLE
#' @description FUNCTION_DESCRIPTION
#' @param conn PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details https://evs.nci.nih.gov/evs-download/thesaurus-downloads
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[pg13]{send}},\code{\link[pg13]{dropTable}}
#'  \code{\link[SqlRender]{render}}
#' @rdname load_mrconso
#' @family nci evs schema
#' @export
#' @importFrom pg13 send dropTable
#' @importFrom SqlRender render


load_all_mth <-
        function(umls_conn,
                 conn) {

                # umls_conn <- metaorite::connectLocalMT()
                # conn <- chariot::connectAthena()

                secretary::typewrite("Warning: this process will take hours and will drop the existing Tables")
                secretary::press_enter()

                schema <- "mth"

                pg_schemas <- pg13::lsSchema(conn = conn)

                if (schema %in% pg_schemas) {

                        Sys.sleep(1)
                        secretary::typewrite(secretary::timepunch(),"\t", "Dropping", schema, "schema...")
                        pg13::send(conn = conn,
                                   sql_statement = SqlRender::render("DROP SCHEMA @schema CASCADE;",
                                                                     schema = schema))
                }

                Sys.sleep(1)
                secretary::typewrite(secretary::timepunch(),"\t", "Creating", schema, "schema...")
                pg13::send(conn = conn,
                           sql_statement =
                                   SqlRender::render(
                                           "CREATE SCHEMA @schema;",
                                           schema = schema))

                Sys.sleep(1)
                secretary::typewrite(secretary::timepunch(),"\t", "DDL", schema, "schema...")
                pg13::send(conn = conn,
                        SqlRender::render(
                                "CREATE TABLE @schema.MRCOLS (
                                        COL	varchar(40),
                                        DES	varchar(200),
                                        REF	varchar(40),
                                        MIN	integer,
                                        AV	numeric(5,2),
                                        MAX	integer,
                                        FIL	varchar(50),
                                        DTY	varchar(40)
                                );

                                CREATE TABLE @schema.MRCONSO (
                                        CUI	char(8) NOT NULL,
                                        LAT	char(3) NOT NULL,
                                        TS	char(1) NOT NULL,
                                        LUI	varchar(10) NOT NULL,
                                        STT	varchar(3) NOT NULL,
                                        SUI	varchar(10) NOT NULL,
                                        ISPREF	char(1) NOT NULL,
                                        AUI	varchar(9) NOT NULL,
                                        SAUI	varchar(50),
                                        SCUI	varchar(100),
                                        SDUI	varchar(100),
                                        SAB	varchar(40) NOT NULL,
                                        TTY	varchar(40) NOT NULL,
                                        CODE	varchar(100) NOT NULL,
                                        STR	text NOT NULL,
                                        SRL	integer NOT NULL,
                                        SUPPRESS	char(1) NOT NULL,
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRCUI (
                                        CUI1	char(8) NOT NULL,
                                        VER	varchar(10) NOT NULL,
                                        REL	varchar(4) NOT NULL,
                                        RELA	varchar(100),
                                        MAPREASON	text,
                                        CUI2	char(8),
                                        MAPIN	char(1)
                                );

                                CREATE TABLE @schema.MRCXT (
                                        CUI	char(8),
                                        SUI	varchar(10),
                                        AUI	varchar(9),
                                        SAB	varchar(40),
                                        CODE	varchar(100),
                                        CXN	integer,
                                        CXL	char(3),
                                        MRCXTRANK	integer,
                                        CXS	text,
                                        CUI2	char(8),
                                        AUI2	varchar(9),
                                        HCD	varchar(100),
                                        RELA	varchar(100),
                                        XC	varchar(1),
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRDEF (
                                        CUI	char(8) NOT NULL,
                                        AUI	varchar(9) NOT NULL,
                                        ATUI	varchar(11) NOT NULL,
                                        SATUI	varchar(50),
                                        SAB	varchar(40) NOT NULL,
                                        DEF	text NOT NULL,
                                        SUPPRESS	char(1) NOT NULL,
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRDOC (
                                        DOCKEY	varchar(50) NOT NULL,
                                        VALUE	varchar(200),
                                        TYPE	varchar(50) NOT NULL,
                                        EXPL	text
                                );

                                CREATE TABLE @schema.MRFILES (
                                        FIL	varchar(50),
                                        DES	varchar(200),
                                        FMT	text,
                                        CLS	integer,
                                        RWS	integer,
                                        BTS	bigint
                                );

                                CREATE TABLE @schema.MRHIER (
                                        CUI	char(8) NOT NULL,
                                        AUI	varchar(9) NOT NULL,
                                        CXN	integer NOT NULL,
                                        PAUI	varchar(10),
                                        SAB	varchar(40) NOT NULL,
                                        RELA	varchar(100),
                                        PTR	text,
                                        HCD	varchar(100),
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRHIST (
                                        CUI	char(8),
                                        SOURCEUI	varchar(100),
                                        SAB	varchar(40),
                                        SVER	varchar(40),
                                        CHANGETYPE	text,
                                        CHANGEKEY	text,
                                        CHANGEVAL	text,
                                        REASON	text,
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRMAP (
                                        MAPSETCUI	char(8) NOT NULL,
                                        MAPSETSAB	varchar(40) NOT NULL,
                                        MAPSUBSETID	varchar(10),
                                        MAPRANK	integer,
                                        MAPID	varchar(50) NOT NULL,
                                        MAPSID	varchar(50),
                                        FROMID	varchar(50) NOT NULL,
                                        FROMSID	varchar(50),
                                        FROMEXPR	text NOT NULL,
                                        FROMTYPE	varchar(50) NOT NULL,
                                        FROMRULE	text,
                                        FROMRES	text,
                                        REL	varchar(4) NOT NULL,
                                        RELA	varchar(100),
                                        TOID	varchar(50),
                                        TOSID	varchar(50),
                                        TOEXPR	text,
                                        TOTYPE	varchar(50),
                                        TORULE	text,
                                        TORES	text,
                                        MAPRULE	text,
                                        MAPRES	text,
                                        MAPTYPE	varchar(50),
                                        MAPATN	varchar(100),
                                        MAPATV	text,
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRRANK (
                                        MRRANK_RANK	integer NOT NULL,
                                        SAB	varchar(40) NOT NULL,
                                        TTY	varchar(40) NOT NULL,
                                        SUPPRESS	char(1) NOT NULL
                                );

                                CREATE TABLE @schema.MRREL (
                                        CUI1	char(8) NOT NULL,
                                        AUI1	varchar(9),
                                        STYPE1	varchar(50) NOT NULL,
                                        REL	varchar(4) NOT NULL,
                                        CUI2	char(8) NOT NULL,
                                        AUI2	varchar(9),
                                        STYPE2	varchar(50) NOT NULL,
                                        RELA	varchar(100),
                                        RUI	varchar(10) NOT NULL,
                                        SRUI	varchar(50),
                                        SAB	varchar(40) NOT NULL,
                                        SL	varchar(40) NOT NULL,
                                        RG	varchar(10),
                                        DIR	varchar(1),
                                        SUPPRESS	char(1) NOT NULL,
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRSAB (
                                        VCUI	char(8),
                                        RCUI	char(8),
                                        VSAB	varchar(40) NOT NULL,
                                        RSAB	varchar(40) NOT NULL,
                                        SON	text NOT NULL,
                                        SF	varchar(40) NOT NULL,
                                        SVER	varchar(40),
                                        VSTART	char(8),
                                        VEND	char(8),
                                        IMETA	varchar(10) NOT NULL,
                                        RMETA	varchar(10),
                                        SLC	text,
                                        SCC	text,
                                        SRL	integer NOT NULL,
                                        TFR	integer,
                                        CFR	integer,
                                        CXTY	varchar(50),
                                        TTYL	varchar(400),
                                        ATNL	text,
                                        LAT	char(3),
                                        CENC	varchar(40) NOT NULL,
                                        CURVER	char(1) NOT NULL,
                                        SABIN	char(1) NOT NULL,
                                        SSN	text NOT NULL,
                                        SCIT	text NOT NULL
                                );

                                CREATE TABLE @schema.MRSAT (
                                        CUI	char(8) NOT NULL,
                                        LUI	varchar(10),
                                        SUI	varchar(10),
                                        METAUI	varchar(100),
                                        STYPE	varchar(50) NOT NULL,
                                        CODE	varchar(100),
                                        ATUI	varchar(11) NOT NULL,
                                        SATUI	varchar(50),
                                        ATN	varchar(100) NOT NULL,
                                        SAB	varchar(40) NOT NULL,
                                        ATV	text,
                                        SUPPRESS	char(1) NOT NULL,
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRSMAP (
                                        MAPSETCUI	char(8) NOT NULL,
                                        MAPSETSAB	varchar(40) NOT NULL,
                                        MAPID	varchar(50) NOT NULL,
                                        MAPSID	varchar(50),
                                        FROMEXPR	text NOT NULL,
                                        FROMTYPE	varchar(50) NOT NULL,
                                        REL	varchar(4) NOT NULL,
                                        RELA	varchar(100),
                                        TOEXPR	text,
                                        TOTYPE	varchar(50),
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRSTY (
                                        CUI	char(8) NOT NULL,
                                        TUI	char(4) NOT NULL,
                                        STN	varchar(100) NOT NULL,
                                        STY	varchar(50) NOT NULL,
                                        ATUI	varchar(11) NOT NULL,
                                        CVF	integer
                                );

                                CREATE TABLE @schema.MRAUI (
                                        AUI1	varchar(9) NOT NULL,
                                        CUI1	char(8) NOT NULL,
                                        VER	varchar(10) NOT NULL,
                                        REL	varchar(4),
                                        RELA	varchar(100),
                                        MAPREASON	text NOT NULL,
                                        AUI2	varchar(9) NOT NULL,
                                        CUI2	char(8) NOT NULL,
                                        MAPIN	char(1) NOT NULL
                                );

                                CREATE TABLE @schema.AMBIGSUI (
                                        SUI	varchar(10) NOT NULL,
                                        CUI	char(8) NOT NULL
                                );

                                CREATE TABLE @schema.AMBIGLUI (
                                        LUI	varchar(10) NOT NULL,
                                        CUI	char(8) NOT NULL
                                );

                                CREATE TABLE @schema.DELETEDCUI (
                                        PCUI	char(8) NOT NULL,
                                        PSTR	text NOT NULL
                                );

                                CREATE TABLE @schema.DELETEDLUI (
                                        PLUI	varchar(10) NOT NULL,
                                        PSTR	text NOT NULL
                                );

                                CREATE TABLE @schema.DELETEDSUI (
                                        PSUI	varchar(10) NOT NULL,
                                        LAT	char(3) NOT NULL,
                                        PSTR	text NOT NULL
                                );

                                CREATE TABLE @schema.MERGEDCUI (
                                        PCUI	char(8) NOT NULL,
                                        CUI	char(8) NOT NULL
                                );

                                CREATE TABLE @schema.MERGEDLUI (
                                        PLUI	varchar(10),
                                        LUI	varchar(10)
                                );

                                CREATE TABLE @schema.MRXNS_ENG (
                                        LAT	char(3) NOT NULL,
                                        NSTR	text NOT NULL,
                                        CUI	char(8) NOT NULL,
                                        LUI	varchar(10) NOT NULL,
                                        SUI	varchar(10) NOT NULL
                                );

                                CREATE TABLE @schema.MRXNW_ENG (
                                        LAT	char(3) NOT NULL,
                                        NWD	varchar(100) NOT NULL,
                                        CUI	char(8) NOT NULL,
                                        LUI	varchar(10) NOT NULL,
                                        SUI	varchar(10) NOT NULL
                                );

                                CREATE TABLE @schema.MRXW_ENG (
                                        LAT	char(3) NOT NULL,
                                        WD	varchar(200) NOT NULL,
                                        CUI	char(8) NOT NULL,
                                        LUI	varchar(10) NOT NULL,
                                        SUI	varchar(10) NOT NULL
                                );",
                                schema = schema)
                )


                mth_tables <- c('MRCOLS', 'MRCONSO', 'MRCUI', 'MRCXT', 'MRDEF', 'MRDOC', 'MRFILES', 'MRHIER', 'MRHIST', 'MRMAP', 'MRRANK', 'MRREL', 'MRSAB', 'MRSAT', 'MRSMAP', 'MRSTY', 'MRAUI', 'AMBIGSUI', 'AMBIGLUI', 'DELETEDCUI', 'DELETEDLUI', 'DELETEDSUI', 'MERGEDCUI', 'MERGEDLUI', 'MRXNS_ENG', 'MRXNW_ENG', 'MRXW_ENG')

                mysql_tables <-
                        preQL::query(
                                conn = umls_conn,
                                sql_statement = "SHOW TABLES;"
                        ) %>%
                        unlist() %>%
                        toupper()


                tables_to_migrate <-
                        mysql_tables[mysql_tables %in% mth_tables]

                Sys.sleep(1)
                secretary::typewrite(secretary::timepunch(),"\tThe following tables will be migrated:")
                tables_to_migrate %>%
                        purrr::map(secretary::typewrite, tabs = 3)
                cat("\n")

                Sys.sleep(3)
                for (i in 1:length(tables_to_migrate)) {

                        table_to_migrate <- tables_to_migrate[i]


                        Sys.sleep(3)
                        secretary::typewrite(secretary::timepunch(),"\tReading",table_to_migrate, "...")
                        data <-
                                preQL::query(conn = umls_conn,
                                             sql_statement = SqlRender::render("SELECT * FROM @table_to_migrate;", table_to_migrate = table_to_migrate))

                        metadata <-
                                pg13::query(conn = conn,
                                            sql_statement = SqlRender::render("SELECT * FROM @schema.@table_to_migrate;", table_to_migrate = table_to_migrate, schema = schema)) %>%
                                colnames() %>%
                                toupper() # all MTH out of MySQL in all uppercase while psql is lowercase

                        Sys.sleep(3)
                        secretary::typewrite(secretary::timepunch(),"\tProcessing",table_to_migrate, "...")

                        data2 <-
                                data %>%
                                rubix::rm_multibyte_chars() %>%
                                dplyr::select(tidyselect::all_of(metadata))



                        Sys.sleep(3)
                        secretary::typewrite(secretary::timepunch(),"\tWriting", temp, "...")

                        temp <- tempfile(fileext = ".csv")
                        # temp <- path.expand("~/Desktop/test_mrconso.csv")
                        readr::write_csv(data2,
                                         temp)

                        Sys.sleep(3)
                        secretary::typewrite(secretary::timepunch(),"\tCopying", temp, "to", table_to_migrate)

                        pg13::send(conn = conn,
                                   SqlRender::render(
                                           SqlRender::readSql(system.file(package = "skyscraper", "sql/copy_from_csv.sql")),
                                           tableName = table_to_migrate,
                                           schema = schema,
                                           vocabulary_file = temp))

                        unlink(temp)

                }




                # Sys.sleep(1)
                # secretary::typewrite(secretary::timepunch(),"\tWriting indexes...")

                # pg13::send(conn = conn,
                #            sql_statement =
                #                    SqlRender::render(
                #                         "CREATE INDEX X_MRCONSO_CUI ON @schema.MRCONSO(CUI);
                #                         ALTER TABLE @schema.MRCONSO ADD CONSTRAINT X_MRCONSO_PK PRIMARY KEY (AUI);
                #                         CREATE INDEX X_MRCONSO_SUI ON @schema.MRCONSO(SUI);
                #                         CREATE INDEX X_MRCONSO_LUI ON @schema.MRCONSO(LUI);
                #                         CREATE INDEX X_MRCONSO_CODE ON @schema.MRCONSO(CODE);
                #                         CREATE INDEX X_MRCONSO_SAB_TTY ON @schema.MRCONSO(SAB,TTY);
                #                         CREATE INDEX X_MRCONSO_SCUI ON @schema.MRCONSO(SCUI);
                #                         CREATE INDEX X_MRCONSO_STR ON @schema.MRCONSO(STR);",
                #                         schema = schema))


                load_log <-
                        data.frame(ll_datetime = Sys.time(),
                                   ll_table =  tableName)


                Tables <- lsTables(conn = conn,
                                   schema = schema)

                if ("LOAD_LOG" %in% Tables) {

                        pg13::appendTable(conn = conn,
                                          schema = schema,
                                          tableName = "LOAD_LOG",
                                          load_log)

                } else {

                        pg13::writeTable(conn = conn,
                                         schema = schema,
                                         tableName = "LOAD_LOG",
                                         load_log)
                }

                secretary::typewrite(secretary::timepunch(),"\t MTH successfully loaded to ", schema, " and LOAD_LOG updated.")

        }

