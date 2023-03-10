CREATE OR REPLACE PACKAGE APPS_BICC_UTIL_PKG IS
    /* $Header: APPS_BICC_UTIL_PKG.pks  ver 1.0 06.01.2023 $
    +==========================================================================+
    |                   Copyright (c) 2023. Apps Akademi                       |
    |                        All rights reserved                               |
    +==========================================================================+
    |                                                                          |
    |  FILENAME                                                                |
    |  APPS_BICC_UTIL_PKG.pks                                                  |
    |                                                                          |
    |  DESCRIPTION    Used to process the flat files which are                 |
    |            extracted from Oracle Fusion Business Intelligence Connector  |
    +==========================================================================+
    */
    -- UCM'den alınmış MANIFEST.MF dosyalarının "İşlenmeye Hazır" statüsünü belirtir, bu dosyalardan henüz flat file dosya bilgileri extract edilmemiş
    G_PENDING_MANIFEST_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Manifest Okundu';
    -- Manifest dosyasından flat file bilgileri okunma işlemi tamamlandı statü bilgisi
    G_DONE_MANIFEST_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Tamamlandi';
    -- Flat file için indirme işlemi bekleniyor statü bilgisi
    G_PENDING_FLAT_FILE_DOWNLOAD_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Indirme Bekleniyor';
    -- Flat file indirildikten sonra işlenmeyi beklerken alacağı statü bilgisi
    G_FLAT_FILE_DOWNLOADED_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Indirildi, Islenme Bekleniyor';
    -- Flat file zip'ten çıkarıldıktan sonra işlenmeyi beklerken alacağı statü bilgisi
    G_FLAT_FILE_UNZIPPED_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Tamamlandi';
    -- Flat file okunup ana tabloya yazıldıktan sonra alacağı statü
    G_FLAT_FILE_COMPLETED_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Isleme Tamamlandi';
    -- Flat file ile işimiz bittikten sonra silindiğinde alacağı statü
    G_FLAT_FILE_DELETED_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Tamamlandi';
    -- Flat file'ların yazılacağı işletim sistemi dizini
    G_FLAT_FILE_DOWNLOAD_DIR VARCHAR2(4000) := '/oraarch/external';
    -- Dosyaları okurken birkaç satırlık kayıplar aldığımızı farkettik, aşağıdaki parametre default olarak 500000 civarında olmasından kaynaklanıyormuş
    -- Sistem kaynakları baz alınarak bu değer değiştirilebilir, şu anda 500 MB sularında seyrediyor
    G_EXT_TABLE_READSIZE NUMBER := 500000000;
    G_EXT_TABLE_DIR_NAME VARCHAR2(400) := 'EXT_TABLE_DIR';
    G_EXT_TABLE_NAME VARCHAR2(400) := 'APPS_BICC_EXT_TEMP';
    FUNCTION GET_LOOKUP_CONSTANT(P_LOOKUP_TYPE IN VARCHAR2, P_LOOKUP_CODE IN VARCHAR2) RETURN VARCHAR2;
    PROCEDURE MANAGE_MANIFESTS(P_DOCUMENT_ID IN VARCHAR2, P_FORCE_IF_PROCESSED IN VARCHAR2);
    PROCEDURE MANAGE_FLAT_FILE(P_DOCUMENT_ID IN VARCHAR2, P_STATUS_CODE OUT VARCHAR2);
END;