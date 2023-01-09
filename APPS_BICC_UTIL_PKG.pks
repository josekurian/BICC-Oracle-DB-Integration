CREATE OR REPLACE PACKAGE APPS_BICC_UTIL_PKG IS
    /* $Header: APPS_BICC_UTIL_PKG.pks  ver 1.0 06.01.2023 $
    +==========================================================================+
    |                        Copyright (c) 2023. Apps Akademi                  |
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
    G_DONE_MANIFEST_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Tamamlandı';
    -- Flat file için indirme işlemi bekleniyor statü bilgisi
    G_PENDING_FLAT_FILE_DOWNLOAD_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Indirme Bekleniyor';
    -- Flat file indirildikten sonra işlenmeyi beklerken alacağı statü bilgisi
    G_FLAT_FILE_DOWNLOADED_STATUS APPS_BICC_EXT_FILES.STATUS%TYPE := 'Indirildi';
    -- Flat file'ların yazılacağı işletim sistemi dizini
    G_FLAT_FILE_DOWNLOAD_DIR VARCHAR2(4000) := '/oradata/external';
    -- Zip dosyasını extract edecek script
    G_SH_UNZIP_SCRIPT VARCHAR2(4000) := 'unzip -o ';
    -- Script dosya adı
    G_SH_UNZIP_SCRIPT_FILENAME VARCHAR2(4000) := 'unzip.sh';
    -- Script yazılacak olan lokasyon
    G_SH_UNZIP_SCRIPT_DIRECTORY VARCHAR2(4000) := G_FLAT_FILE_DOWNLOAD_DIR;
    FUNCTION GET_LOOKUP_CONSTANT(P_LOOKUP_TYPE IN VARCHAR2, P_LOOKUP_CODE IN VARCHAR2) RETURN VARCHAR2;
    PROCEDURE MANAGE_MANIFESTS(P_DOCUMENT_ID IN VARCHAR2, P_FORCE_IF_PROCESSED IN VARCHAR2);
END;