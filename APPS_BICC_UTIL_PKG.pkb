CREATE OR REPLACE PACKAGE BODY APPS_BICC_UTIL_PKG IS
    /* $Header: APPS_BICC_UTIL_PKG.pkb  ver 1.0 06.01.2023 $
    +==========================================================================+
    |                        Copyright (c) 2023. Apps Akademi                  |
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
    FUNCTION GET_LOOKUP_CONSTANT(P_LOOKUP_TYPE IN VARCHAR2, P_LOOKUP_CODE IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        IF P_LOOKUP_TYPE = 'GLOBAL' THEN
            IF P_LOOKUP_CODE = 'PENDING_MANIFEST_STATUS' THEN
                RETURN G_PENDING_MANIFEST_STATUS;
            ELSIF P_LOOKUP_CODE = 'PENDING_FLAT_FILE_DOWNLOAD_STATUS' THEN
                RETURN G_PENDING_FLAT_FILE_DOWNLOAD_STATUS;
            ELSIF P_LOOKUP_CODE = 'FLAT_FILE_DOWNLOAD_DIR' THEN
                RETURN G_FLAT_FILE_DOWNLOAD_DIR;
            ELSIF P_LOOKUP_CODE = 'FLAT_FILE_DOWNLOADED_STATUS' THEN
                RETURN G_FLAT_FILE_DOWNLOADED_STATUS;
            END IF;
        END IF;
    END GET_LOOKUP_CONSTANT;
    PROCEDURE MANAGE_MANIFESTS(P_DOCUMENT_ID IN VARCHAR2, P_FORCE_IF_PROCESSED IN VARCHAR2) IS
        C_PROCEDURE_AUDIT_NAME VARCHAR2(400) := '(APPS_BICC_UTIL_PKG.MANAGE_MANIFESTS) ' || P_DOCUMENT_ID;
        C_NEWLINE_CHARACTER    VARCHAR2(400) := chr(10);
        C_MANIFEST_COLUMN_SEPARATOR   VARCHAR2(400) := ';';
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
                        C_PROCEDURE_AUDIT_NAME AS CREATED_BY,
                        sysdate AS LAST_UPDATE_DATE,
                        C_PROCEDURE_AUDIT_NAME AS LAST_UPDATED_BY,
                        G_PENDING_FLAT_FILE_DOWNLOAD_STATUS AS STATUS
                    FROM
                        RAW_ROWS;

                    -- Manifest dosyası işlendi olarak güncelleniyor
                    UPDATE APPS_BICC_EXT_FILES
                    SET
                        STATUS = G_DONE_MANIFEST_STATUS,
                        LAST_UPDATE_DATE = sysdate,
                        LAST_UPDATED_BY = C_PROCEDURE_AUDIT_NAME
                    WHERE
                        DOCUMENT_ID = REC_MNFST.DOCUMENT_ID;
                END;
            END LOOP;
        COMMIT;
    END MANAGE_MANIFESTS;
END;