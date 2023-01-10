CREATE TABLE SONER_TEMP (
    InventoryTransactionPEOTransactionId NUMBER,
    InventoryTransactionPEOCreatedBy VARCHAR2(4000),
    InventoryTransactionPEOTransactionDate TIMESTAMP,
    InventoryTransactionPEOCreationDate TIMESTAMP
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY "EXT_TABLE_DIR"
    ACCESS PARAMETERS (
        records delimited  by newline
        CHARACTERSET AL32UTF8
        FIELD NAMES FIRST FILE
        fields  terminated by ','
        optionally enclosed by '"'
        missing field values are null (
            InventoryTransactionPEOTransactionId,
            InventoryTransactionPEOCreatedBy,
            InventoryTransactionPEOTransactionDate char(26)  date_format TIMESTAMP MASK "yyyy-mm-dd hh24:mi:ss.ff6",
            InventoryTransactionPEOCreationDate char(26)  date_format TIMESTAMP MASK "yyyy-mm-dd hh24:mi:ss.ff6"
        )
    )
    LOCATION (
      "EXT_TABLE_DIR":'file_fscmtopmodelam_scmextractam_invbiccextractam_inventorytransactiondetailextractpvo-batch1485712016-20230110_105330.csv'
    )
)
REJECT LIMIT UNLIMITED;