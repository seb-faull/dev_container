import pandas as pd  # type: ignore
import argparse
import yaml  # type: ignore
from templates import models
from templates import source_yamls
from src.adapters.filesystems import LocalFilesystem, BaseFilesystem
from src.logger import log
import io
from configuration.invalid_table_names import invalid_table_names  # type: ignore
from typing import Dict, Any
from src.ddl_generators import RawDDLGenerator, TempRawDDLGenerator, CuratedDDLGenerator


class MyDumper(yaml.Dumper):
    def increase_indent(self, flow=False, indentless=False):
        yaml.Dumper.ignore_aliases = lambda *args: True
        return super(MyDumper, self).increase_indent(flow, False)


def column_lists_dict(
    columns: list, template: str = "{column}", join_with: str = ","
) -> str:
    new_columns = (template.format(**column) for column in columns)
    return "{}".format(join_with).join(new_columns)


def column_lists(
    columns: list[Dict[str, Any]], template: str = "{column}", join_with: str = ","
) -> str:
    new_columns = (template.format(column=column) for column in columns)
    return "{}".format(join_with).join(new_columns)


def check_for_invalid_table_names(
    table_names: list[str], invalid_names: list[str] = invalid_table_names
) -> None:
    log.debug("Checking for invalid table names")
    collisions = [
        invalid_name for invalid_name in table_names if invalid_name in invalid_names
    ]
    if collisions:
        log.warning("Identified invalid table names in provided list.")
        raise ValueError(f"Input table list contains invalid table names: {collisions}")
    return


def master(
    action: str,
    database: str,
    schema: str,
    input_file: str,
    output_folder: str,
    source_short_name: str,
    local_filesystem: BaseFilesystem = LocalFilesystem(),
) -> None:
    # https://stackoverflow.com/questions/51272814/python-yaml-dumping-pointer-references
    yaml.Dumper.ignore_aliases = lambda *args: True

    raw_schema = schema.upper()
    # ToDo: _LAKE needs to be dynamic and passed in
    # Morning Andy, so for the RAW we suffix it with the location of where the data came from. _LAKE for the data lake
    #  or _DIRECT if it comes in direct from the source.
    raw_database = "RAW_" + database.upper() + "_LAKE"
    curated_schema = schema.upper()
    curated_database = "CUR_" + database.upper()

    # load meta data
    input_csv_content = local_filesystem.read_file(filepath=input_file)
    df = pd.read_csv(io.StringIO(input_csv_content), sep=",")

    # create Column for DDL ToDo: add function to translate source_data_type to snowflake data type
    df["ddl_column_def"] = df["column_name"] + " " + df["source_data_type"]

    model_scd2_table_list = []
    raw_model_table_list = []
    source_table_list = []
    # ToDo: What is DEMO_DEPUPLICATION_CUR_C_UDKEY_2 and what do we use for X_DEAL_CALC_RESULT or should it be a scd2
    # X_DEAL_CALC_MSG looks like a type 2

    table_names = df["table_name"].unique()
    check_for_invalid_table_names(table_names=table_names)

    # loop each table
    for table_name in table_names:
        # if table_name != 'C_CONTACT_UDF_LOOKUP':
        #     continue
        df_filtered = df[df["table_name"] == table_name]  # handy filtered df
        # ToDo: handle duplicate table descriptions and curated_dbt_type, because thats now allowed.
        table_description = df_filtered["table_description"].unique()[0]
        curated_dbt_type = df_filtered["curated_dbt_type"].unique()[0]
        table_name = table_name.upper()
        raw_model_name = (
            "raw_"
            + source_short_name.lower()
            + "_"
            + schema.lower()
            + "_"
            + table_name.lower()
        )
        cur_model_name = (
            "cur_"
            + source_short_name.lower()
            + "_"
            + schema.lower()
            + "_"
            + table_name.lower()
        )
        raw_source_yaml_filename = (
            "raw_" + source_short_name.lower() + "_" + schema.lower() + ".yml"
        )
        cur_source_yaml_filename = (
            "cur_" + source_short_name.lower() + "_" + schema.lower() + ".yml"
        )

        # output raw temp DDL
        raw_temp_ddl = TempRawDDLGenerator.run(
            raw_database=raw_database, raw_schema=raw_schema, table_name=table_name
        )
        local_filesystem.write_file(
            filepath=f"{output_folder}/output_ddl/raw/temp_{table_name.lower()}.sql",
            content=raw_temp_ddl + "\n",
        )

        # output raw DDL
        raw_ddl = RawDDLGenerator.run(
            raw_database=raw_database, raw_schema=raw_schema, table_name=table_name
        )
        local_filesystem.write_file(
            filepath=f"{output_folder}/output_ddl/raw/{table_name.lower()}.sql",
            content=raw_ddl + "\n",
        )

        RAW_MODEL_output = models.RAW_MODEL.format(
            raw_database=raw_database,
            raw_schema=raw_schema,
            table_name=table_name.lower(),
        )
        local_filesystem.write_file(
            filepath=f"{output_folder}/output_dbt/raw_models/{raw_model_name}.sql",
            content=RAW_MODEL_output + "\n",
        )

        # output dbt curated
        if curated_dbt_type == "scd2":  # drop in meta for this
            unique_key = ""
            updated_date = ""
            # scd2 yaml
            model_scd2_column_list = []
            unique_key_list = []
            updated_date_list = []

            for row in df_filtered.itertuples():
                column_desc = {
                    "name": row.column_name,
                    "description": row.column_description,
                    "tests": [row.column_tests],
                }
                model_scd2_column_list.append(column_desc)

                if row.column_type == "unique_key":
                    unique_key_list.append(row.column_name)
                if row.column_type == "updated_date":
                    updated_date_list.append(row.column_name)

            # scd2 .sql files
            model_scd2_column_list = df_filtered.to_dict("records")
            print(model_scd2_column_list)
            model_scd2_column_string = column_lists_dict(
                columns=model_scd2_column_list,
                template="    cast(FILECONTENTS:{column_name} as {source_data_type}) as {column_name}",
                join_with=",\n",
            )

            # ok now we have looped through all the columns, we can work out if we need to create a
            # concatinated unique_key
            if len(unique_key_list) > 1:
                create_unique_key = (
                    column_lists(
                        columns=unique_key_list,
                        template="FILECONTENTS:{column}",
                        join_with=" || '_' || ",
                    )
                    + " as unique_key"
                )
                model_scd2_column_string = (
                    "    " + create_unique_key + ",\n" + model_scd2_column_string
                )
                unique_key = "unique_key"
            else:
                unique_key = unique_key_list[0]

            if len(updated_date_list) > 1:
                create_updated_date = "GREATEST(" + (
                    column_lists(
                        columns=updated_date_list,
                        template="IFNULL(TO_TIMESTAMP_NTZ(FILECONTENTS:{column}),'1900-01-01')",
                        join_with=",  ",
                    )
                    + ") as updated_date"
                )
                model_scd2_column_string = (
                    "    " + create_updated_date + ",\n" + model_scd2_column_string
                )
                updated_date = "updated_date"
            else:
                updated_date = updated_date_list[0]

            CURATED_MODEL_output = models.CURATED_MODEL.format(
                curated_database=curated_database,
                curated_schema=curated_schema,
                table_name=table_name,
                columns=model_scd2_column_string,
                unique_key=unique_key,
                updated_date=updated_date,
                raw_model_name=raw_model_name,
            )
            local_filesystem.write_file(
                filepath=f"{output_folder}/output_dbt/cur_models/{cur_model_name}.sql",
                content=CURATED_MODEL_output + "\n",
            )

        # output curated DDL
        curated_ddl = CuratedDDLGenerator.run(
            curated_database=curated_database,
            curated_schema=curated_schema,
            table_name=table_name,
            column_df=df_filtered,
        )
        local_filesystem.write_file(
            filepath=f"{output_folder}/output_ddl/curated/{table_name.lower()}.sql",
            content=curated_ddl + "\n",
        )

        # source yaml
        source_yaml = source_yamls.SOURCE_YAML_TABLE
        source_yaml["name"] = table_name
        source_yaml["description"] = table_description
        source_table_list.append(source_yaml.copy())

        # raw yaml
        column_list = []

        for row in source_yaml["columns"]:
            column_tests_source = row["tests"]
            column_tests_source_list = []

            column_desc = {
                "name": row["name"],
                "description": row["description"],
            }
            if column_tests_source:
                column_tests_source_list = column_tests_source.split(",")
                column_desc |= {
                    "tests": column_tests_source_list,
                }
            column_list.append(column_desc)

        table_desc = {
            "name": raw_model_name,
            "description": table_description,
            "columns": column_list,
        }

        # join all of the table dicts to my table list
        raw_model_table_list.append(table_desc)

        # scd2 yaml
        column_list = []

        # csv file parser only accepts "unique,footest,bartest" as format
        # tried a few combinations but it kept complaining
        # so type is string (if not nan) then i convert to list

        for row in df_filtered.itertuples():
            column_tests_scd2 = row.column_tests
            column_tests_scd2_list = []

            column_desc = {
                "name": row.column_name,
                "description": row.column_description,
            }
            if column_tests_scd2 == column_tests_scd2:
                column_tests_scd2_list = column_tests_scd2.split(",")
                column_desc |= {
                    "tests": column_tests_scd2_list,
                }
            column_list.append(column_desc)

        if len(unique_key_list) > 1:
            column_desc = {
                "name": "unique_key",
                "description": "generated column for curated scd2",
            }
            column_list.insert(0, column_desc)

        if len(updated_date_list) > 1:
            column_desc = {
                "name": "updated_date",
                "description": "generated column for curated scd2",
            }
            column_list.insert(0, column_desc)

        # add in the default scd2 columns
        # "ROW_START_AT","ROW_END_AT","ROW_IS_CURRENT","TRACKING_HASH"
        scd2_default_columns = [
            "TRACKING_HASH",
            "ROW_START_AT",
            "ROW_END_AT",
            "ROW_IS_CURRENT",
        ]
        for col in scd2_default_columns:
            column_desc = {"name": col, "description": "SCD2 standard column"}
            column_list.append(column_desc)

        table_desc = {
            "name": cur_model_name,
            "description": table_description,
            "columns": column_list,
        }

        # join all of the table dicts to my table list
        model_scd2_table_list.append(table_desc)

    # output dbt raw yaml
    raw_model_yml = {"version": 2, "models": raw_model_table_list}
    # ToDo: handle when there are no scd2!!
    local_filesystem.write_file(
        filepath=f"{output_folder}/output_dbt/raw_model_yaml/{raw_source_yaml_filename}",
        content=yaml.dump(
            raw_model_yml,
            Dumper=MyDumper,
            sort_keys=False,
            default_flow_style=False,
        ),
    )

    # output dbt scd2 yaml
    model_scd2_yml = {"version": 2, "models": model_scd2_table_list}
    # ToDo: handle when there are no scd2!!
    local_filesystem.write_file(
        filepath=f"{output_folder}/output_dbt/cur_model_yaml/{cur_source_yaml_filename}",
        content=yaml.dump(
            model_scd2_yml,
            Dumper=MyDumper,
            sort_keys=False,
            default_flow_style=False,
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate snowflake objects json and dbt objects"
    )

    parser.add_argument(
        "-a",
        "--action",
        type=str,
        choices=["generate_all_objects", "generate_raw_ddl"],
        help='The action the script should execute, must be one of "generate_config", "generate_curated"',
    )

    parser.add_argument(
        "--database",
        type=str,
        help="""
            Name of database where source tables are. Optional as can be passed in via .env file. Value
            passed in will take precedence over database set in .env file""",
    )

    parser.add_argument(
        "--schema",
        type=str,
        help="""
            Name of schemas where source tables are. Optional as can be passed in via .env file. Value
            passed in will take precedence over database set in .env file""",
    )

    parser.add_argument(
        "-i",
        "--input_filepath",
        type=str,
        help="""
            Path to the input CSV, which contains the metadata to drive the generator. Expected CSV header is:
            table_name,table_description,curated_dbt_type,column_name,column_description,source_data_type,column_type,
            column_tests
            """,
    )

    parser.add_argument(
        "-o",
        "--output_folder",
        default="/workspaces/adp-development-workspace/generators/generator_files/output_files/",
        type=str,
        help="""Optional. Folderpath to output the result to. Should NOT include the trailing '/'. Will default to
        './output'""",
    )

    parser.add_argument(
        "--source_short_name",
        type=str,
        help="""
            Short name of the source, e.g. Alliant
            """,
    )
    args = parser.parse_args()

    master(
        action=args.action,
        database=args.database,
        schema=args.schema,
        input_file=args.input_filepath,
        output_folder=args.output_folder,
        source_short_name=args.source_short_name,
    )
