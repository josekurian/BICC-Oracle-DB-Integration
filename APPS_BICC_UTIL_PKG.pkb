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
            ELSE
                RETURN NULL;
            END IF;
        END IF;
    END GET_LOOKUP_CONSTANT;
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

    PROCEDURE MANAGE_FLAT_FILE(P_DOCUMENT_ID IN VARCHAR2) IS
        P_DOCUMENT_ID                     APPS_BICC_EXT_FILES.DOCUMENT_ID%TYPE := 2506337;
        L_EXT_FILE_ROW                    APPS_BICC_EXT_FILES%ROWTYPE;
        L_STATUS_CODE                     VARCHAR2(4000);
        L_EXT_TABLE_COLUMN_SQL_UPPER_PART CLOB;
        L_EXT_TABLE_COLUMN_SQL_LOWER_PART CLOB;
        L_EXT_TABLE_NAME                  VARCHAR2(400)                        := 'SONER_TEMP';
        L_EXT_TABLE_DIR_NAME              VARCHAR2(400)                        := 'EXT_TABLE_DIR';
        L_EXT_TABLE_CREATE_SQL            CLOB;
        CURSOR CUR_COL_DEF(P_FILE_NAME IN VARCHAR2) IS
            SELECT
                COL_NAME,
                COL_DATATYPE,
                COL_SIZE,
                COL_PRECISION,
                decode(
                        COL_DATATYPE, 'VARCHAR', ('VARCHAR2(' || COL_SIZE || ')'),
                        'NUMERIC', ('NUMBER(' || COL_SIZE || ',' || COL_PRECISION || ')'),
                        'TIMESTAMP', COL_DATATYPE,
                        'DATE', 'COL_DATATYPE',
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
                P_FILE_NAME LIKE '%' || replace(lower(VO_NAME), '.', '_') || '%'
            ORDER BY COL_DATATYPE
        ;
    BEGIN
        SELECT
            *
        INTO L_EXT_FILE_ROW
        FROM
            APPS_BICC_EXT_FILES
        WHERE
            DOCUMENT_ID = P_DOCUMENT_ID;

        FOR REC_COL_DEF IN CUR_COL_DEF(L_EXT_FILE_ROW.FILE_NAME)
            LOOP
                L_EXT_TABLE_COLUMN_SQL_UPPER_PART := L_EXT_TABLE_COLUMN_SQL_UPPER_PART || REC_COL_DEF.COL_NAME;
                L_EXT_TABLE_COLUMN_SQL_UPPER_PART := L_EXT_TABLE_COLUMN_SQL_UPPER_PART || ' ';
                L_EXT_TABLE_COLUMN_SQL_UPPER_PART := L_EXT_TABLE_COLUMN_SQL_UPPER_PART || REC_COL_DEF.SC_DT_PART;
                L_EXT_TABLE_COLUMN_SQL_UPPER_PART := L_EXT_TABLE_COLUMN_SQL_UPPER_PART || ',';

                L_EXT_TABLE_COLUMN_SQL_LOWER_PART := L_EXT_TABLE_COLUMN_SQL_LOWER_PART || REC_COL_DEF.SC_CSV_PART;
                L_EXT_TABLE_COLUMN_SQL_LOWER_PART := L_EXT_TABLE_COLUMN_SQL_LOWER_PART || ',';
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
                                    CHARACTERSET AL32UTF8
                                    FIELD NAMES FIRST FILE
                                    fields  terminated by '',''
                                    optionally enclosed by ''"''
                                    missing field values are null (' || chr(13) ||
                                  L_EXT_TABLE_COLUMN_SQL_LOWER_PART || chr(13) ||
                                  ')
                              )
                              LOCATION (' || chr(13) ||
                                  '"' || L_EXT_TABLE_DIR_NAME || '": ''' || L_EXT_FILE_ROW.FILE_NAME || '''' ||
                                  chr(13) ||
                                  ')
                                )
                                REJECT LIMIT UNLIMITED';

        DBMS_OUTPUT.PUT_LINE('DROP TABLE ' || L_EXT_TABLE_NAME);
        DBMS_OUTPUT.PUT_LINE(L_EXT_TABLE_CREATE_SQL);
        --DBMS_OUTPUT.PUT_LINE(L_EXT_TABLE_COLUMN_SQL_LOWER_PART);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN NULL;
    END;
END;