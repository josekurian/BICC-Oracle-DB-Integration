CREATE OR REPLACE PACKAGE BODY APPS_BICC_UTIL_PKG IS
    /* $Header: APPS_BICC_UTIL_PKG.pkb  ver 1.0 06.01.2023 $
    +==========================================================================+
    |                   Copyright (c) 2023. Apps Akademi                       |
    |                        All rights reserved                               |
    +==========================================================================+
    |                                                                          |
    |  FILENAME                                                                |
    |  APPS_BICC_UTIL_PKG.pkb                                                  |
    |                                                                          |
    |  DESCRIPTION    Used to process the flat files which are                 |
    |            extracted from Oracle Fusion Business Intelligence Connector  |
    +==========================================================================+
    */
    -- OIC'den de aynı değişkenlere ulaşılabilmek amacıyla kullanılacak
    FUNCTION GET_LOOKUP_CONSTANT(P_LOOKUP_TYPE IN VARCHAR2, P_LOOKUP_CODE IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        IF P_LOOKUP_TYPE = 'GLOBAL' THEN
            IF P_LOOKUP_CODE = 'PENDING_MANIFEST_STATUS' THEN
                RETURN APPS_BICC_UTIL_PKG.G_PENDING_MANIFEST_STATUS;
            ELSIF P_LOOKUP_CODE = 'PENDING_FLAT_FILE_DOWNLOAD_STATUS' THEN
                RETURN APPS_BICC_UTIL_PKG.G_PENDING_FLAT_FILE_DOWNLOAD_STATUS;
            ELSIF P_LOOKUP_CODE = 'FLAT_FILE_DOWNLOAD_DIR' THEN
                RETURN APPS_BICC_UTIL_PKG.G_FLAT_FILE_DOWNLOAD_DIR;
            ELSIF P_LOOKUP_CODE = 'FLAT_FILE_DOWNLOADED_STATUS' THEN
                RETURN APPS_BICC_UTIL_PKG.G_FLAT_FILE_DOWNLOADED_STATUS;
            ELSIF P_LOOKUP_CODE = 'FLAT_FILE_UNZIPPED_STATUS' THEN
                RETURN APPS_BICC_UTIL_PKG.G_FLAT_FILE_UNZIPPED_STATUS;
            ELSIF P_LOOKUP_CODE = 'FLAT_FILE_DELETED_STATUS' THEN
                RETURN APPS_BICC_UTIL_PKG.G_FLAT_FILE_DELETED_STATUS;
            ELSIF P_LOOKUP_CODE = 'FLAT_FILE_COMPLETED_STATUS' THEN
                RETURN APPS_BICC_UTIL_PKG.G_FLAT_FILE_COMPLETED_STATUS;
            ELSE
                RETURN NULL;
            END IF;
        END IF;
    END GET_LOOKUP_CONSTANT;

    -- Ana tablodaki kolon isimlerini underscore case'e dönüştürmek için kullanılacak
    FUNCTION BEAUTIFY_COL_NAME(P_COL_NAME IN VARCHAR2, P_VO_NAME IN VARCHAR2) RETURN VARCHAR2 IS
        L_BEAUTIFIED_COL_NAME VARCHAR2(4000);
        CURSOR CUR_RDN_PREF IS
            SELECT
                regexp_substr(REDNDNT_PREFIXES, '[^,]+', 1, LEVEL) AS REDNDNT_PREFIX
            FROM
                APPS_BICC_TAB_DEF
            WHERE
                VO_NAME = P_VO_NAME
            CONNECT BY
                regexp_substr(REDNDNT_PREFIXES, '[^,]+', 1, LEVEL) IS NOT NULL
        ;
    BEGIN
        L_BEAUTIFIED_COL_NAME := P_COL_NAME;
        -- Kolon isimlerini uzatan gereksiz ön ekler siliniyor
        FOR REC_RDN_PREF IN CUR_RDN_PREF
            LOOP
                L_BEAUTIFIED_COL_NAME := replace(L_BEAUTIFIED_COL_NAME, REC_RDN_PREF.REDNDNT_PREFIX, '');
            END LOOP;
        -- Pascal case'den underscore case'e dönüştürülüyor, kolon ismi büyütülüyor
        L_BEAUTIFIED_COL_NAME := upper(regexp_replace(L_BEAUTIFIED_COL_NAME, '([A-Z])', '_\1', 2));
        RETURN L_BEAUTIFIED_COL_NAME;
    END BEAUTIFY_COL_NAME;

    -- Ana tablonun CREATE TABLE script'ini verir
    FUNCTION GET_MAIN_TABLE_CREATE_STMT(P_FILE_NAME IN CLOB) RETURN CLOB IS
        L_TAB_DEF    APPS_BICC_TAB_DEF%ROWTYPE;
        L_CREATE_DDL CLOB;
        CURSOR CUR_COL_DEF IS
            SELECT
                COL_NAME,
                COL_DATATYPE,
                COL_SIZE,
                COL_PRECISION,
                decode(
                        COL_DATATYPE, 'VARCHAR', ('VARCHAR2(' || (to_number(COL_SIZE) + 30)  || ' BYTE)'),
                        'NUMERIC', ('NUMBER' || '(*,' || 8 || ')'),
                        'TIMESTAMP', COL_DATATYPE,
                        'DATE', COL_DATATYPE,
                        (COL_DATATYPE || '(' || COL_SIZE || ',' || COL_PRECISION || ')')
                    ) AS DDL_DATATYPE
            FROM
                APPS_BICC_TAB_COLUMNS
            WHERE
                    lower(P_FILE_NAME) LIKE '%' || replace(lower(VO_NAME), '.', '_') || '%'
        ;
    BEGIN
        SELECT
            *
        INTO
            L_TAB_DEF
        FROM
            APPS_BICC_TAB_DEF
        WHERE
                lower(P_FILE_NAME) LIKE '%' || replace(lower(VO_NAME), '.', '_') || '%';
        L_CREATE_DDL := 'CREATE TABLE ' || L_TAB_DEF.TAB_NAME || ' (';
        FOR REC_COL_DEF IN CUR_COL_DEF
            LOOP

                L_CREATE_DDL := L_CREATE_DDL || BEAUTIFY_COL_NAME(REC_COL_DEF.COL_NAME, L_TAB_DEF.VO_NAME) || ' ' ||
                                REC_COL_DEF.DDL_DATATYPE || ',';
            END LOOP;
        L_CREATE_DDL := rtrim(L_CREATE_DDL, ',');
        L_CREATE_DDL := L_CREATE_DDL || ')';
        RETURN L_CREATE_DDL;
    END GET_MAIN_TABLE_CREATE_STMT;

    -- Ana tabloda varolan ve yeniden gelmiş kayıtlar için silme işlemi yapan DELETE statement'ını verir
    FUNCTION GET_DELETE_EXISTING_STMT(P_FILE_NAME IN CLOB) RETURN CLOB
        IS
        L_IX           NUMBER        := 0;
        L_DELETE_STMT  CLOB;
        L_EXT_TAB_NAME VARCHAR2(255) := G_EXT_TABLE_NAME;
        CURSOR CUR_KEYS IS
            SELECT
                COL.COL_NAME AS EXT_COL_NAME,
                TAB.TAB_NAME,
                COL.KEY_POSITION,
                COL.VO_NAME
            FROM
                APPS_BICC_TAB_COLUMNS COL
                LEFT OUTER JOIN APPS_BICC_TAB_DEF TAB ON COL.VO_NAME = TAB.VO_NAME
            WHERE
                  COL.KEY_TYPE = 'PK'
              AND lower(P_FILE_NAME) LIKE '%' || replace(lower(COL.VO_NAME), '.', '_') || '%'
            ORDER BY COL.KEY_POSITION;
    BEGIN
        FOR REC_KEYS IN CUR_KEYS
            LOOP
                IF L_IX = 0 THEN
                    L_DELETE_STMT := 'DELETE FROM ' || REC_KEYS.TAB_NAME || ' MAIN WHERE EXISTS (';
                    L_DELETE_STMT := L_DELETE_STMT || ' SELECT 1 FROM ' || L_EXT_TAB_NAME || ' EXT_TAB WHERE 1=1';
                END IF;
                L_DELETE_STMT := L_DELETE_STMT || ' AND MAIN.' || BEAUTIFY_COL_NAME(P_COL_NAME => REC_KEYS.EXT_COL_NAME,
                                                                                    P_VO_NAME => REC_KEYS.VO_NAME) ||
                                 ' = EXT_TAB.' ||
                                 REC_KEYS.EXT_COL_NAME;
                L_IX := L_IX + 1;
            END LOOP;
        IF L_DELETE_STMT IS NOT NULL THEN
            L_DELETE_STMT := L_DELETE_STMT || ')';
        END IF;
        RETURN L_DELETE_STMT;
    END GET_DELETE_EXISTING_STMT;

    -- External table'daki tüm veriyi ana tabloya insert eden cümleyi verir
    FUNCTION GET_INSERT_ALL_STMT(P_FILE_NAME IN CLOB) RETURN CLOB IS
        L_IX           NUMBER        := 0;
        L_INSERT_STMT  CLOB;
        L_SELECT_STMT  CLOB;
        L_EXT_TAB_NAME VARCHAR2(255) := G_EXT_TABLE_NAME;
        CURSOR CUR_COLS IS
            SELECT
                COL.COL_NAME AS EXT_COL_NAME,
                TAB.TAB_NAME,
                COL.VO_NAME
            FROM
                APPS_BICC_TAB_COLUMNS COL
                LEFT OUTER JOIN APPS_BICC_TAB_DEF TAB ON COL.VO_NAME = TAB.VO_NAME
            WHERE
                    lower(P_FILE_NAME) LIKE '%' || replace(lower(COL.VO_NAME), '.', '_') || '%'
        ;
    BEGIN
        FOR REC_COLS IN CUR_COLS
            LOOP
                IF L_IX = 0 THEN
                    L_INSERT_STMT := 'INSERT INTO ' || REC_COLS.TAB_NAME || ' (';
                    L_SELECT_STMT := 'SELECT ';
                END IF;
                L_INSERT_STMT :=
                            L_INSERT_STMT || ' ' || BEAUTIFY_COL_NAME(REC_COLS.EXT_COL_NAME, REC_COLS.VO_NAME) || ',';
                L_SELECT_STMT := L_SELECT_STMT || ' ' || REC_COLS.EXT_COL_NAME || ',';
                L_IX := L_IX + 1;
            END LOOP;

        L_SELECT_STMT := rtrim(L_SELECT_STMT, ',');
        L_INSERT_STMT := rtrim(L_INSERT_STMT, ',');

        IF L_INSERT_STMT IS NOT NULL AND L_SELECT_STMT IS NOT NULL THEN
            L_INSERT_STMT := L_INSERT_STMT || ')';
            L_SELECT_STMT := L_SELECT_STMT || ' FROM ' || L_EXT_TAB_NAME;
        END IF;
        RETURN L_INSERT_STMT || ' ' || L_SELECT_STMT;
    END GET_INSERT_ALL_STMT;

    -- External table oluşturma statement'ını verir
    FUNCTION GET_EXTERNAL_TABLE_CREATE_STMT(P_FILE_NAME IN VARCHAR2) RETURN CLOB IS
        L_EXT_TABLE_COLUMN_SQL_UPPER_PART CLOB;
        L_EXT_TABLE_COLUMN_SQL_LOWER_PART CLOB;
        L_EXT_TABLE_NAME                  VARCHAR2(400) := G_EXT_TABLE_NAME;
        L_EXT_TABLE_DIR_NAME              VARCHAR2(400) := G_EXT_TABLE_DIR_NAME;
        L_EXT_TABLE_CREATE_SQL            CLOB;
        CURSOR CUR_COL_DEF IS
            SELECT
                COL_NAME,
                COL_DATATYPE,
                COL_SIZE,
                COL_PRECISION,
                decode(
                        COL_DATATYPE, 'VARCHAR', ('VARCHAR2(' || (to_number(COL_SIZE) + 30) || ' BYTE)'),
                        'NUMERIC', ('NUMBER' || '(*,' || 8 || ')'),
                        'TIMESTAMP', COL_DATATYPE,
                        'DATE', COL_DATATYPE,
                        (COL_DATATYPE || '(' || COL_SIZE || ',' || COL_PRECISION || ')')
                    ) AS SC_DT_PART,
                decode(
                        COL_DATATYPE, 'TIMESTAMP',
                        (COL_NAME || ' char(26)  date_format TIMESTAMP MASK "yyyy-mm-dd hh24:mi:ss.ff6"'),
                        'DATE', (COL_NAME || ' char(26)  date_format TIMESTAMP MASK "yyyy-mm-dd hh24:mi:ss.ff6"'),
                        COL_NAME
                    ) AS SC_CSV_PART
            FROM
                APPS_BICC_TAB_COLUMNS
            WHERE
                    lower(P_FILE_NAME) LIKE '%' || replace(lower(VO_NAME), '.', '_') || '%'
            ORDER BY COL_DATATYPE
        ;
    BEGIN
        -- External table oluşturma DDL'i build ediliyor
        FOR REC_COL_DEF IN CUR_COL_DEF
            LOOP
                L_EXT_TABLE_COLUMN_SQL_UPPER_PART := L_EXT_TABLE_COLUMN_SQL_UPPER_PART || REC_COL_DEF.COL_NAME || ' ' ||
                                                     REC_COL_DEF.SC_DT_PART || ',';
                L_EXT_TABLE_COLUMN_SQL_LOWER_PART :=
                            L_EXT_TABLE_COLUMN_SQL_LOWER_PART || REC_COL_DEF.SC_CSV_PART || ',';
            END LOOP;
        L_EXT_TABLE_COLUMN_SQL_UPPER_PART := rtrim(L_EXT_TABLE_COLUMN_SQL_UPPER_PART, ',');
        L_EXT_TABLE_COLUMN_SQL_LOWER_PART := rtrim(L_EXT_TABLE_COLUMN_SQL_LOWER_PART, ',');

        L_EXT_TABLE_CREATE_SQL := 'CREATE TABLE ' || L_EXT_TABLE_NAME || '(' || chr(13) ||
                                  L_EXT_TABLE_COLUMN_SQL_UPPER_PART || chr(13) ||
                                  ')' || chr(13) ||
                                  'ORGANIZATION EXTERNAL (
                                    TYPE ORACLE_LOADER
                                    DEFAULT DIRECTORY "' || L_EXT_TABLE_DIR_NAME || '"
                                ACCESS PARAMETERS (
                                    records delimited  by newline
                                    SKIP 0
                                    READSIZE ' || G_EXT_TABLE_READSIZE || '
                                    NOLOGFILE
                                    NOBADFILE
                                    NODISCARDFILE
                                    CHARACTERSET AL32UTF8
                                    FIELD NAMES FIRST FILE
                                    fields  terminated by '',''
                                    optionally enclosed by ''"''
                                    missing field values are null (' || chr(13) ||
                                  L_EXT_TABLE_COLUMN_SQL_LOWER_PART || chr(13) ||
                                  ')
                              )
                              LOCATION (' || chr(13) ||
                                  '"' || L_EXT_TABLE_DIR_NAME || '": ''' || P_FILE_NAME || '''' ||
                                  chr(13) ||
                                  ')
                                )
                                REJECT LIMIT 0';
        RETURN L_EXT_TABLE_CREATE_SQL;
    END GET_EXTERNAL_TABLE_CREATE_STMT;

    -- Manifest dosyalarından flat file bilgilerini okuyacak
    PROCEDURE MANAGE_MANIFESTS(P_DOCUMENT_ID IN VARCHAR2, P_FORCE_IF_PROCESSED IN VARCHAR2) IS
        C_PROCEDURE_AUDIT_NAME      VARCHAR2(400) := '(APPS_BICC_UTIL_PKG.MANAGE_MANIFESTS)';
        L_PROCEDURE_AUDIT_NAME      VARCHAR2(400) := C_PROCEDURE_AUDIT_NAME;
        C_NEWLINE_CHARACTER         VARCHAR2(400) := chr(10);
        C_MANIFEST_COLUMN_SEPARATOR VARCHAR2(400) := ';';
        CURSOR CUR_MNFST IS
            SELECT
                *
            FROM
                APPS_BICC_EXT_FILES FL
            WHERE
                  (FL.DOCUMENT_ID = P_DOCUMENT_ID OR P_DOCUMENT_ID IS NULL)
              AND decode(P_FORCE_IF_PROCESSED, 'Y', 1, decode(FL.STATUS, G_PENDING_MANIFEST_STATUS, 1, 0)) = 1
        ;
    BEGIN
        FOR REC_MNFST IN CUR_MNFST
            LOOP
                BEGIN
                    L_PROCEDURE_AUDIT_NAME := C_PROCEDURE_AUDIT_NAME || ' ' || REC_MNFST.DOCUMENT_ID;
                    -- Manifest içerisindeki flat file bilgileri okunup tabloya giriliyor
                    INSERT
                    INTO APPS_BICC_EXT_FILES(
                        DOCUMENT_ID,
                        FILE_NAME,
                        CONTENT,
                        CREATION_DATE,
                        CREATED_BY,
                        LAST_UPDATE_DATE,
                        LAST_UPDATED_BY,
                        STATUS
                    )
                    WITH RAW_CONTENT (S) AS (
                        SELECT
                            to_clob(CONTENT)
                        FROM
                            APPS_BICC_EXT_FILES
                        WHERE
                            DOCUMENT_ID = REC_MNFST.DOCUMENT_ID
                    ),
                         RAW_ROWS AS (
                             SELECT
                                 regexp_substr(S, '[^' || C_NEWLINE_CHARACTER || ']+', 1, LEVEL) AS CONTENT
                             FROM
                                 RAW_CONTENT
                             CONNECT BY
                                 LEVEL <= regexp_count(S, '[^' || C_NEWLINE_CHARACTER || ']+')
                             OFFSET 1 ROW
                         )
                    SELECT
                        regexp_substr(CONTENT, '[^' || C_MANIFEST_COLUMN_SEPARATOR || ']+', 1, 2) AS DOCUMENT_ID,
                        regexp_substr(CONTENT, '[^' || C_MANIFEST_COLUMN_SEPARATOR || ']+', 1, 1) AS FILE_NAME,
                        NULL AS CONTENT,
                        sysdate AS CREATION_DATE,
                        L_PROCEDURE_AUDIT_NAME AS CREATED_BY,
                        sysdate AS LAST_UPDATE_DATE,
                        L_PROCEDURE_AUDIT_NAME AS LAST_UPDATED_BY,
                        G_PENDING_FLAT_FILE_DOWNLOAD_STATUS AS STATUS
                    FROM
                        RAW_ROWS;

                    -- Manifest dosyası işlendi olarak güncelleniyor
                    UPDATE APPS_BICC_EXT_FILES
                    SET
                        STATUS = G_DONE_MANIFEST_STATUS,
                        LAST_UPDATE_DATE = sysdate,
                        LAST_UPDATED_BY = L_PROCEDURE_AUDIT_NAME
                    WHERE
                        DOCUMENT_ID = REC_MNFST.DOCUMENT_ID;
                END;
            END LOOP;
        COMMIT;
    END MANAGE_MANIFESTS;

    /*
        Zip'ten çıkarılıp indirilmiş olan dosyalardan önce external table oluşturacak,
        Ardından bu tablo desenine göre varsa ana tabloya merge yapacak, yoksa oluşturacak
    */
    PROCEDURE MANAGE_FLAT_FILE(P_DOCUMENT_ID IN VARCHAR2, P_STATUS_CODE OUT VARCHAR2) IS
        C_PROCEDURE_AUDIT_NAME   VARCHAR2(400) := '(APPS_BICC_UTIL_PKG.MANAGE_FLAT_FILE)';
        L_PROCEDURE_AUDIT_NAME   VARCHAR2(400) := C_PROCEDURE_AUDIT_NAME;
        L_EXT_FILE_ROW           APPS_BICC_EXT_FILES%ROWTYPE;
        L_TAB_DEF_ROW            APPS_BICC_TAB_DEF%ROWTYPE;
        L_DOES_TAB_EXIST         VARCHAR2(1)   := 'N';
        L_EXT_TABLE_NAME         VARCHAR2(400) := G_EXT_TABLE_NAME;
        L_EXT_TABLE_CREATE_SQL   CLOB;
        L_SHOULD_EXT_TBL_DROP    VARCHAR2(1)   := 'N';
        L_MAIN_TABLE_CREATE_STMT CLOB;
        L_DELETE_EXISTING_STMT   CLOB;
        L_INSERT_ALL_STMT        CLOB;
    BEGIN
        SELECT
            *
        INTO L_EXT_FILE_ROW
        FROM
            APPS_BICC_EXT_FILES
        WHERE
            DOCUMENT_ID = P_DOCUMENT_ID;
        L_PROCEDURE_AUDIT_NAME := C_PROCEDURE_AUDIT_NAME || ' ' || L_EXT_FILE_ROW.DOCUMENT_ID;

        SELECT
            *
        INTO L_TAB_DEF_ROW
        FROM
            APPS_BICC_TAB_DEF
        WHERE
                lower(L_EXT_FILE_ROW.FILE_NAME) LIKE '%' || replace(lower(VO_NAME), '.', '_') || '%';

        IF L_EXT_FILE_ROW.STATUS = G_FLAT_FILE_DOWNLOADED_STATUS THEN


            L_EXT_TABLE_CREATE_SQL := GET_EXTERNAL_TABLE_CREATE_STMT(P_FILE_NAME => L_EXT_FILE_ROW.FILE_NAME);
            -- Staging area görevi görecek olan external table oluşturuluyor (öncelikle varsa drop ediliyor)
            SELECT
                decode(count(*), 0, 'N', 'Y') AS SHOULD_DROP
            INTO L_SHOULD_EXT_TBL_DROP
            FROM
                ALL_EXTERNAL_TABLES
            WHERE
                TABLE_NAME = L_EXT_TABLE_NAME;

            IF L_SHOULD_EXT_TBL_DROP = 'Y' THEN
                EXECUTE IMMEDIATE 'DROP TABLE ' || L_EXT_TABLE_NAME;
            END IF;

            EXECUTE IMMEDIATE L_EXT_TABLE_CREATE_SQL;

            -- Ana tablonun varlığı kontrol ediliyor, yoksa oluşturma DDL'i çalıştırılıyor
            SELECT
                decode(count(*), 0, 'N', 'Y') AS DOES_TAB_EXIST
            INTO L_DOES_TAB_EXIST
            FROM
                USER_TABLES
            WHERE
                TABLE_NAME = L_TAB_DEF_ROW.TAB_NAME;

            L_MAIN_TABLE_CREATE_STMT := GET_MAIN_TABLE_CREATE_STMT(P_FILE_NAME => L_EXT_FILE_ROW.FILE_NAME);

            IF L_DOES_TAB_EXIST = 'N' THEN
                EXECUTE IMMEDIATE L_MAIN_TABLE_CREATE_STMT;
            END IF;

            -- Hem ana tabloda varolan hem de external tabloda yeni gelmiş kayıtlar siliniyor (Güncellenmiş)
            L_DELETE_EXISTING_STMT := GET_DELETE_EXISTING_STMT(P_FILE_NAME => L_EXT_FILE_ROW.FILE_NAME);
            IF L_DELETE_EXISTING_STMT IS NOT NULL THEN
                EXECUTE IMMEDIATE L_DELETE_EXISTING_STMT;
            END IF;

            -- Ana tabloya tüm kayıtlar insert ediliyor
            L_INSERT_ALL_STMT := GET_INSERT_ALL_STMT(P_FILE_NAME => L_EXT_FILE_ROW.FILE_NAME);
            IF L_INSERT_ALL_STMT IS NOT NULL THEN
                EXECUTE IMMEDIATE L_INSERT_ALL_STMT;
            END IF;

            UPDATE
                APPS_BICC_EXT_FILES
            SET
                STATUS = G_FLAT_FILE_COMPLETED_STATUS,
                LAST_UPDATE_DATE = systimestamp,
                LAST_UPDATED_BY = L_PROCEDURE_AUDIT_NAME
            WHERE
                DOCUMENT_ID = L_EXT_FILE_ROW.DOCUMENT_ID;
            COMMIT;
            SELECT
                decode(count(*), 0, 'N', 'Y') AS SHOULD_DROP
            INTO L_SHOULD_EXT_TBL_DROP
            FROM
                ALL_EXTERNAL_TABLES
            WHERE
                TABLE_NAME = L_EXT_TABLE_NAME;

            IF L_SHOULD_EXT_TBL_DROP = 'Y' THEN
                EXECUTE IMMEDIATE 'DROP TABLE ' || L_EXT_TABLE_NAME;
            END IF;
        END IF;
        P_STATUS_CODE := 'S';
    EXCEPTION
        WHEN OTHERS THEN P_STATUS_CODE := DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' Hata: ' || sqlerrm;
    END MANAGE_FLAT_FILE;
END;