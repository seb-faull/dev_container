
{{
  config(
    materialized='from_external_stage',
    stage_name = 'EXT_AWS_ADP/c_status/',
    schema='DBO',
    database='RAW_ALLIANT_PPL_PROD_LAKE',
    alias='c_status'
  )
}}

SELECT
  -1 as ADT_INSERT_AUDIT_ID,
  CURRENT_TIMESTAMP() as ADT_INSERT_TIME,
  NULL as ADT_UPDATE_AUDIT_ID,
  NULL as ADT_UPDATE_TIME,
  TO_DATE(SPLIT_PART(SPLIT_PART(metadata$filename, '/', 4), '=', 2),'YYYYMMDD') AS ADT_BATCH_DATE,
  metadata$filename as ADT_STAGE_FILE_NAME,
  metadata$file_row_number as ADT_STAGE_FILE_ROW,
  NULL as  ADT_METADATA,
  NULL as ADT_BOOKMARK_COLUMN,             /*The name of the column where the bookmark value was extracted from*/
  NULL as ADT_BOOKMARK_TYPE,                   /*Is it a TIMESTAMP, DATE or NUMBER used for bookmarking?*/
  NULL as ADT_BOOKMARK_VALUE,       /*Extract the value of the variant column we'll use for bookmarking,
  converting to varchar to accommodate different data types*/
  $1 as FILECONTENTS
from
  {{ external_stage() }}

