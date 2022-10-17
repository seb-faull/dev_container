import adp_generator
import pytest


class TestADPGenerator:
    def test_master(self, patched_local_filesystem):
        output_folder = "./tests/assets/temp_output"
        asset_folder = "./tests/assets/master"

        adp_generator.master(
            action="generate_all_objects",
            database="ALLIANT_PPL_PROD",
            schema="DBO",
            source_short_name="aln",
            input_file=f"{asset_folder}/asset_input.csv",
            output_folder=output_folder,
            local_filesystem=patched_local_filesystem,
        )

        # test to compare expected output files with generated output files

        # read expected output files

        # read expected alliant curated dbt yaml
        expected_dbt_cur_aln_yaml = patched_local_filesystem.read_file(
            filepath=f"{asset_folder}/expected_output/expected_output_dbt/expected_cur_model_yaml/asset_cur_aln_dbo.yml"
        )

        # read expected alliant curated dbt model
        expected_dbt_cur_aln_model = patched_local_filesystem.read_file(
            filepath=f"{asset_folder}/expected_output/expected_output_dbt/expected_cur_models/"
            "asset_cur_aln_dbo_c_status.sql"
        )

        # read expected alliant raw dbt yaml
        expected_dbt_raw_aln_yaml = patched_local_filesystem.read_file(
            filepath=f"{asset_folder}/expected_output/expected_output_dbt/expected_raw_model_yaml/asset_raw_aln_dbo.yml"
        )

        # read expected alliant raw dbt model
        expected_dbt_raw_aln_model = patched_local_filesystem.read_file(
            filepath=f"{asset_folder}/expected_output/expected_output_dbt/expected_raw_models/"
            "asset_raw_aln_dbo_c_status.sql"
        )

        # read expected alliant curated ddl
        expected_ddl_cur_aln = patched_local_filesystem.read_file(
            filepath=f"{asset_folder}/expected_output/expected_output_ddl/expected_curated/c_status.sql"
        )

        # read expected alliant raw ddl
        expected_ddl_raw_aln = patched_local_filesystem.read_file(
            filepath=f"{asset_folder}/expected_output/expected_output_ddl/expected_raw/c_status.sql"
        )

        # read expected alliant temp raw ddl
        expected_ddl_temp_raw_aln = patched_local_filesystem.read_file(
            filepath=f"{asset_folder}/expected_output/expected_output_ddl/expected_raw/temp_c_status.sql"
        )

        # read generator output files

        # read generated alliant curated dbt yaml
        generated_dbt_cur_aln_yaml = patched_local_filesystem.read_file(
            filepath=f"{output_folder}/output_dbt/cur_model_yaml/cur_aln_dbo.yml"
        )

        # read generated alliant curated dbt model
        generated_dbt_cur_aln_model = patched_local_filesystem.read_file(
            filepath=f"{output_folder}/output_dbt/cur_models/cur_aln_dbo_c_status.sql"
        )

        # read generated alliant raw dbt yaml
        generated_dbt_raw_aln_yaml = patched_local_filesystem.read_file(
            filepath=f"{output_folder}/output_dbt/raw_model_yaml/raw_aln_dbo.yml"
        )

        # read generated alliant raw dbt model
        generated_dbt_raw_aln_model = patched_local_filesystem.read_file(
            filepath=f"{output_folder}/output_dbt/raw_models/raw_aln_dbo_c_status.sql"
        )

        # read generated alliant curated ddl
        generated_ddl_cur_aln = patched_local_filesystem.read_file(
            filepath=f"{output_folder}/output_ddl/curated/c_status.sql"
        )

        # read generated alliant raw ddl
        generated_ddl_raw_aln = patched_local_filesystem.read_file(
            filepath=f"{output_folder}/output_ddl/raw/c_status.sql"
        )

        # read generated alliant temp raw ddl
        generated_ddl_temp_raw_aln = patched_local_filesystem.read_file(
            filepath=f"{output_folder}/output_ddl/raw/temp_c_status.sql"
        )

        # check that expected output equals generated output
        assert expected_dbt_cur_aln_yaml == generated_dbt_cur_aln_yaml
        assert expected_dbt_cur_aln_model == generated_dbt_cur_aln_model
        assert expected_dbt_raw_aln_yaml == generated_dbt_raw_aln_yaml
        assert expected_dbt_raw_aln_model == generated_dbt_raw_aln_model
        assert expected_ddl_cur_aln == generated_ddl_cur_aln
        assert expected_ddl_raw_aln == generated_ddl_raw_aln
        assert expected_ddl_temp_raw_aln == generated_ddl_temp_raw_aln

    def test_check_for_invalid_table_names(self):
        input_name_list = ["table_one", "table_two", "bad_name"]
        invalid_list = ["bad_name"]
        with pytest.raises(
            ValueError,
            match=r"Input table list contains invalid table names: \['bad_name'\]",
        ):
            adp_generator.check_for_invalid_table_names(
                table_names=input_name_list, invalid_names=invalid_list
            )

        input_name_list = [
            "table_one",
            "bad_one",
            "table_two",
            "bad_two",
            "table_three",
        ]
        invalid_list = ["bad_one", "bad_two"]
        with pytest.raises(
            ValueError,
            match=r"Input table list contains invalid table names: \['bad_one', 'bad_two'\]",
        ):
            adp_generator.check_for_invalid_table_names(
                table_names=input_name_list, invalid_names=invalid_list
            )

        input_name_list = [
            "table_one",
            "table_two",
            "table_three",
        ]
        invalid_list = ["bad_one", "bad_two"]
        adp_generator.check_for_invalid_table_names(
            table_names=input_name_list, invalid_names=invalid_list
        )
