RAW_MODEL = """
{{{{
  config(
    materialized='from_external_stage',
    stage_name = 'EXT_AWS_ADP/{table_name}/',
    schema='{raw_schema}',
    database='{raw_database}',
    alias='{table_name}'
  )
}}}}

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
  {{{{ external_stage() }}}}
"""

CURATED_MODEL_OLD = """
{{{{
    config(
      materialized='custom_scd2_rank',
      database='{curated_database}',
      schema='{curated_schema}',
      unique_key='{unique_key}',
      update_date='{updated_date}',
      alias='{table_name}',
      rank_column='RANK_COL',
      pre_hook="begin transaction",
      post_hook="commit",
    )

}}}}

SELECT
    rank() over (partition by {unique_key} order by hash({updated_date}, adt_stage_file_name) asc) rank_col,
    CURRENT_TIMESTAMP() AS ADT_INSERTED_AT,
    NULL AS ADT_UPDATED_AT,
    ADT_BATCH_DATE AS ADT_BATCH_DATE,
    ADT_STAGE_FILE_NAME AS ADT_STAGE_FILE_NAME,
    ADT_STAGE_FILE_ROW AS ADT_STAGE_FILE_ROW,
    ADT_METADATA AS ADT_METADATA,
{columns}
FROM
    {{{{stream(ref('{raw_model_name}'),'str_{table_name}')}}}}
"""


CURATED_MODEL = """
{{{{
    config(
      materialized='custom_scd2_rank',
      database='{curated_database}',
      schema='{curated_schema}',
      unique_key='{unique_key}',
      update_date='{updated_date}',
      alias='{table_name}',
      rank_column='RANK_COL',
      pre_hook="begin transaction",
      post_hook="commit",
    )

}}}}
select
  rank() over (partition by {unique_key} order by {updated_date}, adt_stage_file_name asc) rank_col,
  *
from
(
SELECT
    CURRENT_TIMESTAMP() AS ADT_INSERTED_AT,
    NULL AS ADT_UPDATED_AT,
    ADT_BATCH_DATE AS ADT_BATCH_DATE,
    ADT_STAGE_FILE_NAME AS ADT_STAGE_FILE_NAME,
    ADT_STAGE_FILE_ROW AS ADT_STAGE_FILE_ROW,
    ADT_METADATA AS ADT_METADATA,
{columns}
FROM
    {{{{stream(ref('{raw_model_name}'),'str_{table_name}')}}}}) a
"""
