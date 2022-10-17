from src.input_data_values import ColumnTypes
from src.ddl_generators import CuratedDDLGenerator, RawDDLGenerator, TempRawDDLGenerator
import pandas as pd  # type: ignore
import pytest


class TestTempRawDDLGenerator:
    def test_ddl_generates_correctly(self):
        temp_raw_ddl_generator = TempRawDDLGenerator()

        raw_database = "test_db"
        raw_schema = "test_schema"
        table_name = "test_table"
        expected_output = """
CREATE TRANSIENT TABLE IF NOT EXISTS test_db.test_schema.TEMP_test_table
(
    ADT_INSERT_AUDIT_ID     NUMBER(38,0),
    ADT_INSERT_TIME         TIMESTAMP_NTZ(9),
    ADT_UPDATE_AUDIT_ID     NUMBER(38,0),
    ADT_UPDATE_TIME         TIMESTAMP_NTZ(9),
    ADT_BATCH_DATE          DATE,
    ADT_STAGE_FILE_NAME     VARCHAR(1024),
    ADT_STAGE_FILE_ROW      NUMBER(38,0),
    ADT_METADATA            VARIANT,
    ADT_BOOKMARK_COLUMN     VARCHAR(255),
    ADT_BOOKMARK_TYPE       VARCHAR(20),
    ADT_BOOKMARK_VALUE      VARCHAR(100),
    FILECONTENTS            VARIANT
);"""

        assert (
            temp_raw_ddl_generator.run(
                raw_database=raw_database, raw_schema=raw_schema, table_name=table_name
            )
            == expected_output
        )


class TestRawDDLGenerator:
    def test_ddl_generates_correctly(self):
        raw_ddl_generator = RawDDLGenerator()

        raw_database = "test_db"
        raw_schema = "test_schema"
        table_name = "test_table"
        expected_output = """
CREATE TABLE IF NOT EXISTS test_db.test_schema.test_table
(
    ADT_INSERT_AUDIT_ID     NUMBER(38,0),
    ADT_INSERT_TIME         TIMESTAMP_NTZ(9),
    ADT_UPDATE_AUDIT_ID     NUMBER(38,0),
    ADT_UPDATE_TIME         TIMESTAMP_NTZ(9),
    ADT_BATCH_DATE          DATE,
    ADT_STAGE_FILE_NAME     VARCHAR(1024),
    ADT_STAGE_FILE_ROW      NUMBER(38,0),
    ADT_METADATA            VARIANT,
    ADT_BOOKMARK_COLUMN     VARCHAR(255),
    ADT_BOOKMARK_TYPE       VARCHAR(20),
    ADT_BOOKMARK_VALUE      VARCHAR(100),
    FILECONTENTS            VARIANT
) ;"""

        assert (
            raw_ddl_generator.run(
                raw_database=raw_database, raw_schema=raw_schema, table_name=table_name
            )
            == expected_output
        )


class TestCuratedDDLGenerator:
    def test_validate_column_df(self):
        fake_df = pd.DataFrame(data={"column_name": ["Foo"]})
        column_checklist = ["column_name"]
        CuratedDDLGenerator._validate_column_df(
            df=fake_df, required_columns=column_checklist
        )

        column_checklist = ["column_name", "other_column"]
        with pytest.raises(
            KeyError,
            match=r"The dataframe used to generate the curated DDL does not contain all of the needed columns \(\['column_name', 'other_column'\]\)",  # noqa: E501
        ):
            CuratedDDLGenerator._validate_column_df(
                df=fake_df, required_columns=column_checklist
            )

    def test_input_has_multiple_tagged_columns(self):
        single_key = pd.DataFrame(data={"column_type": ["unique_key"]})
        two_keys = pd.DataFrame(
            data={"column_type": ["unique_key", "something_else", "unique_key"]}
        )
        zero_keys = pd.DataFrame(data={"column_type": ["something_else"]})

        assert (
            CuratedDDLGenerator._input_has_multiple_tagged_columns(
                df=single_key, column_tag=ColumnTypes.unique_key
            )
            is False
        )
        assert (
            CuratedDDLGenerator._input_has_multiple_tagged_columns(
                df=two_keys, column_tag=ColumnTypes.unique_key
            )
            is True
        )
        with pytest.raises(
            ValueError,
            match=r"There is not at least one column tagged as 'unique_key' in the input data.",
        ):
            CuratedDDLGenerator._input_has_multiple_tagged_columns(
                df=zero_keys, column_tag=ColumnTypes.unique_key
            )

    def test_create_columns_block(self):
        short = pd.DataFrame(
            data={"column_name": ["column_a"], "source_data_type": ["VARCHAR(60)"]}
        )
        expected_short = """    COLUMN_A VARCHAR(60)"""

        long = pd.DataFrame(
            data={
                "column_name": ["column_a", "column_b", "column_c"],
                "source_data_type": ["VARCHAR(60)", "VARCHAR(60)", "NUMBER(38,0)"],
            }
        )
        expected_long = """    COLUMN_A VARCHAR(60),
    COLUMN_B VARCHAR(60),
    COLUMN_C NUMBER(38,0)"""

        assert CuratedDDLGenerator._create_columns_block(df=short) == expected_short
        assert CuratedDDLGenerator._create_columns_block(df=long) == expected_long

    def test_should_be_scd2(self):
        is_scd2 = pd.DataFrame(data={"curated_dbt_type": ["scd2", "scd2"]})
        is_not_scd2 = pd.DataFrame(data={"curated_dbt_type": ["other", "other"]})
        empty = pd.DataFrame(data={"curated_dbt_type": [""]})
        contradiction = pd.DataFrame(data={"curated_dbt_type": ["scd2", "other"]})

        assert CuratedDDLGenerator._should_be_scd2(df=is_scd2) is True
        assert CuratedDDLGenerator._should_be_scd2(df=is_not_scd2) is False
        assert CuratedDDLGenerator._should_be_scd2(df=empty) is False
        with pytest.raises(
            ValueError, match=r"Contradicting values for 'curated_dbt_type'"
        ):
            CuratedDDLGenerator._should_be_scd2(df=contradiction)

    def test_ddl_generates_correctly_single_key(self):
        curated_database = "test_db"
        curated_schema = "test_schema"
        table_name = "test_table"

        columns_single = pd.DataFrame(
            data={
                "column_name": ["column_a", "column_b", "column_c"],
                "source_data_type": ["VARCHAR(60)", "VARCHAR(60)", "NUMBER(38,0)"],
                "curated_dbt_type": ["scd2", "scd2", "scd2"],
                "column_type": ["unique_key", "", ""],
            }
        )

        expected_single = """
CREATE TABLE IF NOT EXISTS test_db.test_schema.test_table
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
    COLUMN_A VARCHAR(60),
    COLUMN_B VARCHAR(60),
    COLUMN_C NUMBER(38,0)
) ;"""

        assert (
            CuratedDDLGenerator.run(
                curated_database=curated_database,
                curated_schema=curated_schema,
                table_name=table_name,
                column_df=columns_single,
            )
            == expected_single
        )

        columns_multi = pd.DataFrame(
            data={
                "column_name": ["column_a", "column_b", "column_c"],
                "source_data_type": ["VARCHAR(60)", "VARCHAR(60)", "NUMBER(38,0)"],
                "curated_dbt_type": ["scd2", "scd2", "scd2"],
                "column_type": ["unique_key", "unique_key", ""],
            }
        )

        expected_multi = """
CREATE TABLE IF NOT EXISTS test_db.test_schema.test_table
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
    UNIQUE_KEY VARCHAR,
    COLUMN_A VARCHAR(60),
    COLUMN_B VARCHAR(60),
    COLUMN_C NUMBER(38,0)
) ;"""

        assert (
            CuratedDDLGenerator.run(
                curated_database=curated_database,
                curated_schema=curated_schema,
                table_name=table_name,
                column_df=columns_multi,
            )
            == expected_multi
        )

    def test_ddl_generates_correctly_multi_date_columns(self):
        curated_database = "test_db"
        curated_schema = "test_schema"
        table_name = "test_table"

        columns_single = pd.DataFrame(
            data={
                "column_name": ["column_a", "column_b", "column_c"],
                "source_data_type": ["VARCHAR(60)", "TIMESTAMP_NTZ", "NUMBER(38,0)"],
                "curated_dbt_type": ["scd2", "scd2", "scd2"],
                "column_type": ["unique_key", "updated_date", ""],
            }
        )

        expected_single = """
CREATE TABLE IF NOT EXISTS test_db.test_schema.test_table
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
    COLUMN_A VARCHAR(60),
    COLUMN_B TIMESTAMP_NTZ,
    COLUMN_C NUMBER(38,0)
) ;"""
        assert (
            CuratedDDLGenerator.run(
                curated_database=curated_database,
                curated_schema=curated_schema,
                table_name=table_name,
                column_df=columns_single,
            )
            == expected_single
        )

        columns_multi = pd.DataFrame(
            data={
                "column_name": ["column_a", "column_b", "column_c"],
                "source_data_type": ["VARCHAR(60)", "TIMESTAMP_NTZ", "TIMESTAMP_NTZ"],
                "curated_dbt_type": ["scd2", "scd2", "scd2"],
                "column_type": ["unique_key", "updated_date", "updated_date"],
            }
        )

        expected_multi = """
CREATE TABLE IF NOT EXISTS test_db.test_schema.test_table
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
    UPDATED_DATE TIMESTAMP_NTZ,
    COLUMN_A VARCHAR(60),
    COLUMN_B TIMESTAMP_NTZ,
    COLUMN_C TIMESTAMP_NTZ
) ;"""

        assert (
            CuratedDDLGenerator.run(
                curated_database=curated_database,
                curated_schema=curated_schema,
                table_name=table_name,
                column_df=columns_multi,
            )
            == expected_multi
        )

    def test_ddl_generates_correctly_multi_date_multi_key_columns(self):
        curated_database = "test_db"
        curated_schema = "test_schema"
        table_name = "test_table"

        columns_combo = pd.DataFrame(
            data={
                "column_name": ["column_a", "column_b", "column_c", "column_d"],
                "source_data_type": [
                    "VARCHAR(60)",
                    "VARCHAR(60)",
                    "TIMESTAMP_NTZ",
                    "TIMESTAMP_NTZ",
                ],
                "curated_dbt_type": ["scd2", "scd2", "scd2", "scd2"],
                "column_type": [
                    "unique_key",
                    "unique_key",
                    "updated_date",
                    "updated_date",
                ],
            }
        )

        expected_combo = """
CREATE TABLE IF NOT EXISTS test_db.test_schema.test_table
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
    UNIQUE_KEY VARCHAR,
    UPDATED_DATE TIMESTAMP_NTZ,
    COLUMN_A VARCHAR(60),
    COLUMN_B VARCHAR(60),
    COLUMN_C TIMESTAMP_NTZ,
    COLUMN_D TIMESTAMP_NTZ
) ;"""
        assert (
            CuratedDDLGenerator.run(
                curated_database=curated_database,
                curated_schema=curated_schema,
                table_name=table_name,
                column_df=columns_combo,
            )
            == expected_combo
        )
