from templates.ddls import RAW_TEMP_DDL, RAW_DDL, CURATED_DDL
from src.logger import log
import pandas as pd  # type: ignore
from enum import Enum
from src.input_data_values import ColumnTypes, CuratedDBTType


class TempRawDDLGenerator:
    @staticmethod
    def run(raw_database: str, raw_schema: str, table_name: str):
        log.info(
            f"Generating Temp Raw DDL for '{raw_database}.{raw_schema}.TEMP_{table_name}'"
        )
        template = RAW_TEMP_DDL
        output = template.format(
            raw_database=raw_database, raw_schema=raw_schema, table_name=table_name
        )
        return output


class RawDDLGenerator:
    @staticmethod
    def run(raw_database: str, raw_schema: str, table_name: str):
        log.info(f"Generating Raw DDL for '{raw_database}.{raw_schema}.{table_name}'")
        template = RAW_DDL
        output = template.format(
            raw_database=raw_database, raw_schema=raw_schema, table_name=table_name
        )
        return output


class CuratedDDLRequiredDFColumns(Enum):
    column_name = "column_name"
    source_data_type = "source_data_type"
    column_type = "column_type"
    curated_dbt_type = "curated_dbt_type"


class CuratedDDLGenerator:
    @staticmethod
    def _validate_column_df(
        df: pd.DataFrame,
        required_columns: list[str] = [
            column.value for column in CuratedDDLRequiredDFColumns
        ],
    ) -> None:
        log.debug("Validating column dataframe.")
        if not all(column in df.columns for column in required_columns):
            exc = f"The dataframe used to generate the curated DDL does not contain all of the needed columns \
({required_columns})"
            log.warning(exc)
            raise KeyError(exc)

    @staticmethod
    def _input_has_multiple_tagged_columns(
        df: pd.DataFrame, column_tag: ColumnTypes, suppress_error: bool = False
    ) -> bool:
        log.debug("Identifying number of columns tagged as '{column_tag.value}'.")
        column_count = (
            df[CuratedDDLRequiredDFColumns.column_type.value] == column_tag.value
        ).sum()
        log.debug(f"Input data indicates {column_count} {column_tag.value}/s")
        if column_count < 1:
            exc = f"There is not at least one column tagged as '{column_tag.value}' in the input data."
            if suppress_error:
                log.warning(exc)
            else:
                raise ValueError(exc)
        # Should be able to do this with 'return column_count > 1' but this doesn't return a proper bool since we're
        # involving pandas
        if column_count > 1:
            return True
        return False

    @staticmethod
    def _should_be_scd2(df: pd.DataFrame) -> bool:
        log.debug("Identifying if this should be generated as an SCD2.")
        curated_dbt_type_values = df[
            CuratedDDLRequiredDFColumns.curated_dbt_type.value
        ].unique()
        if len(curated_dbt_type_values) > 1:
            exc = "Contradicting values for 'curated_dbt_type'"
            log.warning(exc)
            raise ValueError(exc)
        if curated_dbt_type_values == [CuratedDBTType.scd2.value]:
            return True
        return False

    @staticmethod
    def _create_columns_block(df: pd.DataFrame) -> str:
        log.debug("Creating 'columns block'.")
        df["ddl_column_def"] = df["column_name"] + " " + df["source_data_type"]
        ddl_column_def_list = df["ddl_column_def"].tolist()
        new_columns = (
            "    {column}".format(column=column.upper())
            for column in ddl_column_def_list
        )
        return "{}".format(",\n").join(new_columns)

    @staticmethod
    def _generate_ddl(
        curated_database: str,
        curated_schema: str,
        table_name: str,
        columns_block: str,
        is_multi_key: bool,
        is_multi_date: bool,
    ) -> str:
        log.debug("Generating DDL.")
        multi_key_row = ""
        multi_date_row = ""
        if is_multi_key:
            multi_key_row = "\n    UNIQUE_KEY VARCHAR,"
        if is_multi_date:
            multi_date_row = "\n    UPDATED_DATE TIMESTAMP_NTZ,"
        template = CURATED_DDL
        return template.format(
            curated_database=curated_database,
            curated_schema=curated_schema,
            table_name=table_name,
            columns=columns_block,
            multi_updated_date=multi_date_row,
            multi_key=multi_key_row,
        )

    @staticmethod
    def run(
        curated_database: str,
        curated_schema: str,
        table_name: str,
        column_df: pd.DataFrame,
    ):
        log.info(
            f"Generating Curated DDL for '{curated_database}.{curated_schema}.{table_name}'"
        )
        CuratedDDLGenerator._validate_column_df(df=column_df)
        if not CuratedDDLGenerator._should_be_scd2(df=column_df):
            raise NotImplementedError(
                "Want to create a non-scd2 curated DDL. This has not been implemented"
            )

        columns_block = CuratedDDLGenerator._create_columns_block(df=column_df)
        is_multi_key = CuratedDDLGenerator._input_has_multiple_tagged_columns(
            df=column_df, column_tag=ColumnTypes.unique_key
        )
        is_multi_date = CuratedDDLGenerator._input_has_multiple_tagged_columns(
            df=column_df, column_tag=ColumnTypes.updated_date, suppress_error=True
        )
        ddl = CuratedDDLGenerator._generate_ddl(
            curated_database=curated_database,
            curated_schema=curated_schema,
            table_name=table_name,
            columns_block=columns_block,
            is_multi_key=is_multi_key,
            is_multi_date=is_multi_date,
        )
        return ddl
