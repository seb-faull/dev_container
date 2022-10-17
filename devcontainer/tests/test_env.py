import os
import pytest
from importlib.metadata import version

current_snowflake_login = os.getenv("SNOWFLAKE_LOGIN")
current_snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
current_snowflake_role = os.getenv("SNOWFLAKE_ROLE")
current_snowflake_database = os.getenv("SNOWFLAKE_DATABASE")
current_snowflake_schema = os.getenv("SNOWFLAKE_SCHEMA")
current_snowflake_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
current_snowflake_host = os.getenv("SNOWFLAKE_HOST")
current_aws_profile = os.getenv("AWS_PROFILE")
current_dbt_profile = os.getenv("DBT_PROFILES_DIR")
# //sets up the current variables in the .env file that pytest will use in its tests


# tests the snowflake host, aws profile and dbt profile configured in test_env.py
# against the default values and identify any changes from the default values

environment_config__to__default_values = {
    "snowflake_host": "ppluk-dev",
    "aws_profile": "ppluk-adp-d",
    "dbt_profiles_dir": "/workspaces/adp-development-workspace/git-repos/adp-dbt-filestore/src/adp/config",
}
# //sets up a dictionary with the default values

environment_config__to__current_values = {
    "snowflake_host": current_snowflake_host,
    "aws_profile": current_aws_profile,
    "dbt_profiles_dir": current_dbt_profile,
}
# //sets up a dictionary populated with the current values from the .env file


def test__environment_config__default_values_equal_current_values():
    assert (
        environment_config__to__default_values == environment_config__to__current_values
    )


# //tests the differences between the default values and the current values


# tests the snowflake role, database, schema and warehouse configured in test_env.py
# against the default snowflake values, the test will identify any changes from the default values

snowflake_config__to__default_values = {
    "snowflake_role": "DEPLOYMENTADMIN",
    "snowflake_database": "UTIL_AUDIT",
    "snowflake_schema": "PUBLIC",
    "snowflake_warehouse": "INGEST__ALLIANT__ALL",
}
# //sets up a dictionary with the default values

snowflake_config__to__current_values = {
    "snowflake_role": current_snowflake_role,
    "snowflake_database": current_snowflake_database,
    "snowflake_schema": current_snowflake_schema,
    "snowflake_warehouse": current_snowflake_warehouse,
}
# //sets up a dictionary populated with the current values from the .env file


def test__snowflake_config__default_values_equal_current_values():
    assert snowflake_config__to__default_values == snowflake_config__to__current_values


# //tests the differences between the default values and the current values


# tests the snowflake login, password, role, database, schema and warehouse variables configured
# in test_env.py to ensure they are not enclosed in double quotes, not empty strings and not None


def get_current_snowflake_variables():
    snowflake_variables = [
        current_snowflake_login,
        current_snowflake_password,
        current_snowflake_role,
        current_snowflake_database,
        current_snowflake_schema,
        current_snowflake_warehouse,
    ]
    for snowflake_variable in snowflake_variables:
        yield snowflake_variable


# //sets up a list of snowflake variables based on the current values in the .env file
# //yield returns a iterable generator object allowing you to test each value in the list


@pytest.mark.parametrize(
    "snowflake_variable_double_quotes__input", get_current_snowflake_variables()
)
def test__snowflake_variables__double_quotes(snowflake_variable_double_quotes__input):
    assert '"' not in snowflake_variable_double_quotes__input


# //tests that variable is not enclosed in double quotes


@pytest.mark.parametrize(
    "snowflake_variable_not_empty__input", get_current_snowflake_variables()
)
def test__snowflake_variables__not_empty(snowflake_variable_not_empty__input):
    snowflake_variable_not_empty__input = snowflake_variable_not_empty__input.strip()
    assert snowflake_variable_not_empty__input != ""


# //tests that the variable is not an empty string


@pytest.mark.parametrize(
    "snowflake_variable_not_none__input", get_current_snowflake_variables()
)
def test__snowflake_variables__not_none(snowflake_variable_not_none__input):
    assert snowflake_variable_not_none__input is not None


# //tests that the variable is not None


# retrieves the version of each package in the container_packages list to identify any packages
# that haven't been installed
def get_container_packages():
    container_packages = ["dbt-snowflake", "snowflake-connector-python"]
    for container_package in container_packages:
        yield container_package


# //yield returns a iterable generator object allowing you to test each value in the list


@pytest.mark.parametrize("container_packages__input", get_container_packages())
def test__container_packages__installed(container_packages__input):
    assert version(container_packages__input)


# //retrieves the version of the package and fails if package is not installed
