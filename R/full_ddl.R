full_ddl <-
        "
                DROP TABLE IF EXISTS @schema.MRCOLS;
                CREATE TABLE @schema.MRCOLS (
                        COL	varchar(40),
                        DES	varchar(200),
                        REF	varchar(40),
                        MIN	integer,
                        AV	numeric(5,2),
                        MAX	integer,
                        FIL	varchar(50),
                        DTY	varchar(40)
                );
                DROP TABLE IF EXISTS @schema.MRCONSO;
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
                DROP TABLE IF EXISTS @schema.MRCUI;
                CREATE TABLE @schema.MRCUI (
                        CUI1	char(8) NOT NULL,
                        VER	varchar(10) NOT NULL,
                        REL	varchar(4) NOT NULL,
                        RELA	varchar(100),
                        MAPREASON	text,
                        CUI2	char(8),
                        MAPIN	char(1)
                );
                DROP TABLE IF EXISTS @schema.MRCXT;
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
                DROP TABLE IF EXISTS @schema.MRDEF;
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
                DROP TABLE IF EXISTS @schema.MRDOC;
                CREATE TABLE @schema.MRDOC (
                        DOCKEY	varchar(50) NOT NULL,
                        VALUE	varchar(200),
                        TYPE	varchar(50) NOT NULL,
                        EXPL	text
                );
                DROP TABLE IF EXISTS @schema.MRFILES;
                CREATE TABLE @schema.MRFILES (
                        FIL	varchar(50),
                        DES	varchar(200),
                        FMT	text,
                        CLS	integer,
                        RWS	integer,
                        BTS	bigint
                );
                DROP TABLE IF EXISTS @schema.MRHIER;
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
                DROP TABLE IF EXISTS @schema.MRHIST;
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
                DROP TABLE IF EXISTS @schema.MRMAP;
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
                DROP TABLE IF EXISTS @schema.MRRANK;
                CREATE TABLE @schema.MRRANK (
                        MRRANK_RANK	integer NOT NULL,
                        SAB	varchar(40) NOT NULL,
                        TTY	varchar(40) NOT NULL,
                        SUPPRESS	char(1) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRREL;
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
                DROP TABLE IF EXISTS @schema.MRSAB;
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
                DROP TABLE IF EXISTS @schema.MRSAT;
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
                DROP TABLE IF EXISTS @schema.MRSMAP;
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
                DROP TABLE IF EXISTS @schema.MRSTY;
                CREATE TABLE @schema.MRSTY (
                        CUI	char(8) NOT NULL,
                        TUI	char(4) NOT NULL,
                        STN	varchar(100) NOT NULL,
                        STY	varchar(50) NOT NULL,
                        ATUI	varchar(11) NOT NULL,
                        CVF	integer
                );
                DROP TABLE IF EXISTS @schema.MRXNS_ENG;
                CREATE TABLE @schema.MRXNS_ENG (
                        LAT	char(3) NOT NULL,
                        NSTR	text NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXNW_ENG;
                CREATE TABLE @schema.MRXNW_ENG (
                        LAT	char(3) NOT NULL,
                        NWD	varchar(100) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRAUI;
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
                DROP TABLE IF EXISTS @schema.MRXW_BAQ;
                CREATE TABLE @schema.MRXW_BAQ (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_CHI;
                CREATE TABLE @schema.MRXW_CHI (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_CZE;
                CREATE TABLE @schema.MRXW_CZE (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_DAN;
                CREATE TABLE @schema.MRXW_DAN (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_DUT;
                CREATE TABLE @schema.MRXW_DUT (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_ENG;
                CREATE TABLE @schema.MRXW_ENG (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_EST;
                CREATE TABLE @schema.MRXW_EST (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_FIN;
                CREATE TABLE @schema.MRXW_FIN (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_FRE;
                CREATE TABLE @schema.MRXW_FRE (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );
                DROP TABLE IF EXISTS @schema.MRXW_GER;
                CREATE TABLE @schema.MRXW_GER (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_GRE;
                CREATE TABLE @schema.MRXW_GRE (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_HEB;
                CREATE TABLE @schema.MRXW_HEB (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_HUN;
                CREATE TABLE @schema.MRXW_HUN (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_ITA;
                CREATE TABLE @schema.MRXW_ITA (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_JPN;
                CREATE TABLE @schema.MRXW_JPN (
                        LAT char(3) NOT NULL,
                        WD  varchar(500) NOT NULL,
                        CUI char(8) NOT NULL,
                        LUI varchar(10) NOT NULL,
                        SUI varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_KOR;
                CREATE TABLE @schema.MRXW_KOR (
                        LAT char(3) NOT NULL,
                        WD  varchar(500) NOT NULL,
                        CUI char(8) NOT NULL,
                        LUI varchar(10) NOT NULL,
                        SUI varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_LAV;
                CREATE TABLE @schema.MRXW_LAV (
                        LAT char(3) NOT NULL,
                        WD  varchar(200) NOT NULL,
                        CUI char(8) NOT NULL,
                        LUI varchar(10) NOT NULL,
                        SUI varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_NOR;
                CREATE TABLE @schema.MRXW_NOR (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_POL;
                CREATE TABLE @schema.MRXW_POL (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_POR;
                CREATE TABLE @schema.MRXW_POR (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_RUS;
                CREATE TABLE @schema.MRXW_RUS (
                        LAT char(3) NOT NULL,
                        WD  varchar(200) NOT NULL,
                        CUI char(8) NOT NULL,
                        LUI varchar(10) NOT NULL,
                        SUI varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_SCR;
                CREATE TABLE @schema.MRXW_SCR (
                        LAT char(3) NOT NULL,
                        WD  varchar(200) NOT NULL,
                        CUI char(8) NOT NULL,
                        LUI varchar(10) NOT NULL,
                        SUI varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_SPA;
                CREATE TABLE @schema.MRXW_SPA (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_SWE;
                CREATE TABLE @schema.MRXW_SWE (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MRXW_TUR;
                CREATE TABLE @schema.MRXW_TUR (
                        LAT	char(3) NOT NULL,
                        WD	varchar(200) NOT NULL,
                        CUI	char(8) NOT NULL,
                        LUI	varchar(10) NOT NULL,
                        SUI	varchar(10) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.AMBIGSUI;
                CREATE TABLE @schema.AMBIGSUI (
                        SUI	varchar(10) NOT NULL,
                        CUI	char(8) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.AMBIGLUI;
                CREATE TABLE @schema.AMBIGLUI (
                        LUI	varchar(10) NOT NULL,
                        CUI	char(8) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.DELETEDCUI;
                CREATE TABLE @schema.DELETEDCUI (
                        PCUI	char(8) NOT NULL,
                        PSTR	text NOT NULL
                );

                DROP TABLE IF EXISTS @schema.DELETEDLUI;
                CREATE TABLE @schema.DELETEDLUI (
                        PLUI	varchar(10) NOT NULL,
                        PSTR	text NOT NULL
                );

                DROP TABLE IF EXISTS @schema.DELETEDSUI;
                CREATE TABLE @schema.DELETEDSUI (
                        PSUI	varchar(10) NOT NULL,
                        LAT	char(3) NOT NULL,
                        PSTR	text NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MERGEDCUI;
                CREATE TABLE @schema.MERGEDCUI (
                        PCUI	char(8) NOT NULL,
                        CUI	char(8) NOT NULL
                );

                DROP TABLE IF EXISTS @schema.MERGEDLUI;
                CREATE TABLE @schema.MERGEDLUI (
                        PLUI	varchar(10),
                        LUI	varchar(10)
                );"
