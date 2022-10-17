
{{
    config(
      materialized='custom_scd2_rank',
      database='CUR_ALLIANT_PPL_PROD',
      schema='DBO',
      unique_key='unique_identity',
      update_date='system_modified_datetime',
      alias='C_STATUS',
      rank_column='RANK_COL',
      pre_hook="begin transaction",
      post_hook="commit",
    )

}}
select
  rank() over (partition by unique_identity order by system_modified_datetime, adt_stage_file_name asc) rank_col,
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
    cast(FILECONTENTS:status_sid as NUMBER(38,0)) as status_sid,
    cast(FILECONTENTS:unique_identity as VARCHAR(50)) as unique_identity,
    cast(FILECONTENTS:status_id as VARCHAR(60)) as status_id,
    cast(FILECONTENTS:descr as VARCHAR(255)) as descr,
    cast(FILECONTENTS:comment as VARCHAR(1000)) as comment,
    cast(FILECONTENTS:system_modified_datetime as TIMESTAMP_NTZ) as system_modified_datetime
FROM
    {{stream(ref('raw_aln_dbo_c_status'),'str_C_STATUS')}}) a

