
CREATE TABLE IF NOT EXISTS CUR_ALLIANT_PPL_PROD.DBO.C_STATUS
(
    ADT_INSERTED_AT TIMESTAMP_NTZ(9),
    ADT_UPDATED_AT TIMESTAMP_NTZ(9),
    ADT_BATCH_DATE DATE,
    ADT_STAGE_FILE_NAME VARCHAR(1024),
    ADT_STAGE_FILE_ROW NUMBER(38,0),
    ADT_METADATA VARIANT,
    ROW_START_AT TIMESTAMP_NTZ,
    ROW_END_AT TIMESTAMP_NTZ,
    ROW_IS_CURRENT CHAR(1),
    TRACKING_HASH VARCHAR,
    STATUS_SID NUMBER(38,0),
    UNIQUE_IDENTITY VARCHAR(50),
    STATUS_ID VARCHAR(60),
    DESCR VARCHAR(255),
    COMMENT VARCHAR(1000),
    SYSTEM_MODIFIED_DATETIME TIMESTAMP_NTZ
) ;
