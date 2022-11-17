from __future__ import division, print_function, unicode_literals
import json
from datetime import datetime
from os import environ as env
import argparse

DISTRIBUTION_MODELS = [
    'model.adp.raw_aln_dbo_x_deal_calc_result',
    'model.adp.cur_aln_dbo_x_deal_calc_result',
    'model.adp.trns_vault_pre_aln_allocation',
    'model.adp.trns_vault_aln_allocation',
    'model.adp.vault_raw_allocation_h',
    'model.adp.vault_raw_allocation_alliant_s',
    'model.adp.vault_raw_allocation_transaction_l',
    'model.adp.pres_landing_fact_dist_allocation',
    'model.adp.pres_landing_fact_dist_payment',
    'model.adp.pres_landing_fact_dist_international',
    'model.adp.pres_landing_fact_agg_dist_member_rec_allocation',
    'model.adp.pres_pre_fact_dist'
    ]

init_dag_no_glue_function = """
def get_glue_job_full_name(glue_job_name):
    stack_string = os.environ.get("AIRFLOW__VAR__ADP_ALLIANT_DATAPIPELINE")
    glue_full_name = stack_string + glue_job_name

    return glue_full_name
"""

GLUE_TASK_GROUP = """
    with TaskGroup(group_id='glue_jobs', default_args={'pool': 'glue_pool'}) as group_glue_jobs:
"""

GLUE_DICT = {}
GLUE_DICT.update({"c_udkey_1_udf":{"airflowe_task_id":"glue_c_udkey_1_udf","glue_job_name":"-raw-load-c-udkey-1-udf"}})
GLUE_DICT.update({"c_udkey_1_udf_lookup":{"airflowe_task_id":"glue_c_udkey_1_udf_lookup","glue_job_name":"-raw-load-c-udkey-1-udf-lkp"}})
GLUE_DICT.update({"x_ip_rights_udf":{"airflowe_task_id":"glue_x_ip_rights_udf","glue_job_name":"-raw-load-x-ip-rights-udf"}})
GLUE_DICT.update({"c_contact_udf":{"airflowe_task_id":"glue_c_contact_udf","glue_job_name":"-raw-load-c-contact-udf"}})
GLUE_DICT.update({"x_ip_rights_status":{"airflowe_task_id":"glue_x_ip_rights_status","glue_job_name":"-raw-load-x-ip-rights-status"}})
GLUE_DICT.update({"x_ip_rights":{"airflowe_task_id":"glue_x_ip_rights","glue_job_name":"-raw-load-x-ip-rights"}})
GLUE_DICT.update({"c_udkey_1":{"airflowe_task_id":"glue_c_udkey_1","glue_job_name":"-raw-load-c-udkey-1"}})
GLUE_DICT.update({"c_udkey_4_group_resolved":{"airflowe_task_id":"glue_c_udkey_4_group_resolved","glue_job_name":"-raw-load-c-udkey-4-grp-rslvd"}})
GLUE_DICT.update({"c_udkey_1_group_dtl":{"airflowe_task_id":"glue_c_udkey_1_group_dtl","glue_job_name":"-raw-load-c-udkey-1-grp-dtl"}})
GLUE_DICT.update({"c_contact_udf_lookup":{"airflowe_task_id":"glue_c_contact_udf_lookup","glue_job_name":"-raw-load-c-contact-udf-lkp"}})
GLUE_DICT.update({"c_contact":{"airflowe_task_id":"glue_c_contact","glue_job_name":"-raw-load-c-contact"}})
GLUE_DICT.update({"c_udkey_4_group_dtl":{"airflowe_task_id":"glue_c_udkey_4_group_dtl","glue_job_name":"-raw-load-c-udkey-4-grp-dtl"}})
GLUE_DICT.update({"x_contract_udf":{"airflowe_task_id":"glue_x_contract_udf","glue_job_name":"-raw-load-x-contract-udf"}})
GLUE_DICT.update({"c_udkey_4_group_hdr":{"airflowe_task_id":"glue_c_udkey_4_group_hdr","glue_job_name":"-raw-load-c-udkey-4-grp-hdr"}})
GLUE_DICT.update({"c_udkey_6_hierarchy_resolved":{"airflowe_task_id":"glue_c_udkey_6_hierarchy_resolved","glue_job_name":"-raw-load-c-udkey-6-hrchy-rslvd"}})
GLUE_DICT.update({"x_deal_calc_msg":{"airflowe_task_id":"glue_x_deal_calc_msg","glue_job_name":"-raw-load-x-deal-calc-msg"}})
GLUE_DICT.update({"c_udkey_7_udf":{"airflowe_task_id":"glue_c_udkey_7_udf","glue_job_name":"-raw-load-c-udkey-7-udf"}})
GLUE_DICT.update({"c_udkey_7":{"airflowe_task_id":"glue_c_udkey_7","glue_job_name":"-raw-load-c-udkey-7"}})
GLUE_DICT.update({"c_udkey_6":{"airflowe_task_id":"glue_c_udkey_6","glue_job_name":"-raw-load-c-udkey-6"}})
GLUE_DICT.update({"c_udkey_9_hierarchy":{"airflowe_task_id":"glue_c_udkey_9_hierarchy","glue_job_name":"-raw-load-c-udkey-9-hrchy"}})
GLUE_DICT.update({"c_udkey_9":{"airflowe_task_id":"glue_c_udkey_9","glue_job_name":"-raw-load-c-udkey-9"}})
GLUE_DICT.update({"c_udkey_1_group_hdr":{"airflowe_task_id":"glue_c_udkey_1_group_hdr","glue_job_name":"-raw-load-c-udkey-1-grp-hdr"}})
GLUE_DICT.update({"c_udkey_9_hierarchy_resolved":{"airflowe_task_id":"glue_c_udkey_9_hierarchy_resolved","glue_job_name":"-raw-load-c-udkey-9-hrchy-rslvd"}})
GLUE_DICT.update({"c_udkey_2":{"airflowe_task_id":"glue_c_udkey_2","glue_job_name":"-raw-load-c-udkey-2"}})
GLUE_DICT.update({"c_status":{"airflowe_task_id":"glue_c_status","glue_job_name":"-raw-load-c-status"}})
GLUE_DICT.update({"c_udf":{"airflowe_task_id":"glue_c_udf","glue_job_name":"-raw-load-c-udf"}})
GLUE_DICT.update({"c_datatype_enumerated_value":{"airflowe_task_id":"glue_c_datatype_enumerated_value","glue_job_name":"-raw-load-c-datatype-enum-val"}})
GLUE_DICT.update({"c_udkey_2_udf_lookup":{"airflowe_task_id":"glue_c_udkey_2_udf_lookup","glue_job_name":"-raw-load-c-udkey-2-udf-lkp"}})
GLUE_DICT.update({"x_contract_list_dtl":{"airflowe_task_id":"glue_x_contract_list_dtl","glue_job_name":"-raw-load-x-contract-list-dtl"}})
GLUE_DICT.update({"c_udkey_4":{"airflowe_task_id":"glue_c_udkey_4","glue_job_name":"-raw-load-c-udkey-4"}})
GLUE_DICT.update({"x_contract_keyword":{"airflowe_task_id":"glue_x_contract_keyword","glue_job_name":"-raw-load-x-contract-keyword"}})
GLUE_DICT.update({"c_udkey_13_udf_lookup":{"airflowe_task_id":"glue_temp_c_udkey_13_udf_lookup","glue_job_name":"-raw-load-c-udkey-13-udf-lkp"}})
GLUE_DICT.update({"x_period":{"airflowe_task_id":"glue_x_period","glue_job_name":"-raw-load-x-period"}})
GLUE_DICT.update({"c_udkey_6_keyword":{"airflowe_task_id":"glue_c_udkey_6_keyword","glue_job_name":"-raw-load-c-udkey-6-keyword"}})
GLUE_DICT.update({"c_udkey_12":{"airflowe_task_id":"glue_c_udkey_12","glue_job_name":"-raw-load-c-udkey-12"}})
GLUE_DICT.update({"x_contract_list_resolved":{"airflowe_task_id":"glue_x_contract_list_resolved","glue_job_name":"-raw-load-x-contract-list-rslvd"}})
GLUE_DICT.update({"x_contract":{"airflowe_task_id":"glue_x_contract","glue_job_name":"-raw-load-x-contract"}})
GLUE_DICT.update({"x_deal":{"airflowe_task_id":"glue_x_deal","glue_job_name":"-raw-load-x-deal"}})
GLUE_DICT.update({"c_udkey_6_hierarchy":{"airflowe_task_id":"glue_c_udkey_6_hierarchy","glue_job_name":"-raw-load-c-udkey-6-hrchy"}})
GLUE_DICT.update({"c_exclusivity_type":{"airflowe_task_id":"glue_c_exclusivity_type","glue_job_name":"-raw-load-c-exclusivity-type"}})
GLUE_DICT.update({"c_udkey_10":{"airflowe_task_id":"glue_c_udkey_10","glue_job_name":"-raw-load-c-udkey-10"}})
GLUE_DICT.update({"c_udkey_8":{"airflowe_task_id":"glue_c_udkey_8","glue_job_name":"-raw-load-c-udkey-8"}})
GLUE_DICT.update({"c_udkey_3":{"airflowe_task_id":"glue_c_udkey_3","glue_job_name":"-raw-load-c-udkey-3"}})
GLUE_DICT.update({"c_contact_type":{"airflowe_task_id":"glue_c_contact_type","glue_job_name":"-raw-load-c-contact-type"}})
GLUE_DICT.update({"c_udkey_18":{"airflowe_task_id":"glue_c_udkey_18","glue_job_name":"-raw-load-c-udkey-18"}})
GLUE_DICT.update({"x_contract_list_hdr":{"airflowe_task_id":"glue_x_contract_list_hdr","glue_job_name":"-raw-load-x-contract-list-hdr"}})
GLUE_DICT.update({"c_rights_type":{"airflowe_task_id":"glue_c_rights_type","glue_job_name":"-raw-load-c-rights-type"}})
GLUE_DICT.update({"c_keyword":{"airflowe_task_id":"glue_c_keyword","glue_job_name":"-raw-load-c-keyword"}})
GLUE_DICT.update({"c_udkey_11":{"airflowe_task_id":"glue_c_udkey_11","glue_job_name":"-raw-load-c-udkey-11"}})
GLUE_DICT.update({"c_udkey_16":{"airflowe_task_id":"glue_c_udkey_16","glue_job_name":"-raw-load-c-udkey-16"}})
GLUE_DICT.update({"c_udkey_13":{"airflowe_task_id":"glue_c_udkey_13","glue_job_name":"-raw-load-c-udkey-13"}})
GLUE_DICT.update({"c_calc":{"airflowe_task_id":"glue_c_calc","glue_job_name":"-raw-load-c-calc"}})
GLUE_DICT.update({"c_udkey_11_hierarchy":{"airflowe_task_id":"glue_c_udkey_11_hierarchy","glue_job_name":"-raw-load-c-udkey-11-hierarchy"}})
GLUE_DICT.update({"c_udkey_15":{"airflowe_task_id":"glue_c_udkey_15","glue_job_name":"-raw-load-c-udkey-15"}})
GLUE_DICT.update({"c_udkey_16_hierarchy":{"airflowe_task_id":"glue_c_udkey_16_hierarchy","glue_job_name":"-raw-load-c-udkey-16-hierarchy"}})
GLUE_DICT.update({"c_udkey_19":{"airflowe_task_id":"glue_c_udkey_19","glue_job_name":"-raw-load-c-udkey-19"}})
GLUE_DICT.update({"c_udkey_19_udf":{"airflowe_task_id":"glue_c_udkey_19_udf","glue_job_name":"-raw-load-c-udkey-19-udf"}})
GLUE_DICT.update({"c_udkey_2_group_dtl":{"airflowe_task_id":"glue_c_udkey_2_group_dtl","glue_job_name":"-raw-load-c-udkey-2-group-dtl"}})
GLUE_DICT.update({"c_udkey_2_group_hdr":{"airflowe_task_id":"glue_c_udkey_2_group_hdr","glue_job_name":"-raw-load-c-udkey-2-group-hdr"}})
GLUE_DICT.update({"c_udkey_5":{"airflowe_task_id":"glue_c_udkey_5","glue_job_name":"-raw-load-c-udkey-5"}})
GLUE_DICT.update({"c_udkey_5_hierarchy":{"airflowe_task_id":"glue_c_udkey_5_hierarchy","glue_job_name":"-raw-load-c-udkey-5-hierarchy"}})
GLUE_DICT.update({"c_udkey_7_group_dtl":{"airflowe_task_id":"glue_c_udkey_7_group_dtl","glue_job_name":"-raw-load-c-udkey-7-group-dtl"}})
GLUE_DICT.update({"c_udkey_7_group_hdr":{"airflowe_task_id":"glue_c_udkey_7_group_hdr","glue_job_name":"-raw-load-c-udkey-7-group-hdr"}})
GLUE_DICT.update({"c_udkey_7_keyword":{"airflowe_task_id":"glue_c_udkey_7_keyword","glue_job_name":"-raw-load-c-udkey-7-keyword"}})
GLUE_DICT.update({"x_deal_udf":{"airflowe_task_id":"glue_x_deal_udf","glue_job_name":"-raw-load-x-deal-udf"}})

# made this up..ToDo - need distribution adding in..
GLUE_DICT.update({"x_deal_calc_result":{"airflowe_task_id":"glue_x_deal_calc_result","glue_job_name":"-raw-load-x-deal-calc-result"}})

def load_manifest():
    local_filepath = f"/workspaces/adp-development-workspace/git-repos/adp-dbt-filestore/src/adp/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
    return data

def clean_name(name):
    return name.replace("$", "__")

def add_dependency(file_string, dependency):
    if dependency not in file_string:
        file_string += dependency
    return file_string

def clean_task_name(task_name):
    return task_name.replace(".", "_")

def make_dbt_task(node_name, has_test):
    test_name = ""
    if has_test:
        test_name = f""", tests="{node_name}" """
        test_vars = """, tests_vars="--exclude tag:test_validate_pii tag:test_relationships" """
    else:
        test_name = f""", tests="" """
        test_vars = """, tests_vars="" """

    dbt_task = f"""ECSOperator(
        task_id="{node_name}",
        cluster=ecs_cluster,
        task_definition=ecs_cluster,
        launch_type="FARGATE",
        aws_conn_id="aws_ecs",
        overrides=overrided(models="{node_name}"{test_name}{test_vars}),
        network_configuration=network_config,
        awslogs_group=logs_group,
        awslogs_stream_prefix=logs_stream_prefix
    )
"""
    return dbt_task

def make_dbt_tag_test_relationships():
    tag_test_relationships = """
    tag_test_relationships = ECSOperator(
        task_id="tag_test_relationships",
        cluster=ecs_cluster,
        task_definition=ecs_cluster,
        launch_type="FARGATE",
        aws_conn_id="aws_ecs",
        overrides=overrided(models="", tests="", tests_vars="tag:test_relationships" ),
        network_configuration=network_config,
        awslogs_group=logs_group,
        awslogs_stream_prefix=logs_stream_prefix
    )
"""
    return tag_test_relationships

def make_glue_task(table_name):
    if table_name != "streams_validation_test":
        glue_info = GLUE_DICT.get(table_name)
        airflowe_task_id = glue_info["airflowe_task_id"]
        glue_job_name = glue_info["glue_job_name"]
        region_name = "eu-west-1"
        dbt_task = f"""{airflowe_task_id} = AwsGlueJobOperator(
                task_id="{airflowe_task_id}",
                job_name=get_glue_job_full_name("{glue_job_name}"),
                job_desc="run glue job " + get_glue_job_full_name("{glue_job_name}"),
                region_name="{region_name}",
                num_of_dpus=1,
                dag=dag,
                aws_conn_id='aws_default'
            )
    """
        return dbt_task

def get_glue_airflow_task_name(table_name):
    if table_name != "streams_validation_test":
        glue_info = GLUE_DICT.get(table_name)
        airflowe_task_id = glue_info["airflowe_task_id"]
        return airflowe_task_id

def generate_imports(file_string=""):
    file_string += """from airflow import DAG
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
import os
    """
    return file_string

def generate_default_configs(file_string="", project_name=""):
    file_string += """
default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2022, 1, 1)
}

network_config = {
    "awsvpcConfiguration": {
        "subnets": [
            os.environ.get("AIRFLOW__VAR__PRIVATE_SUBNET_1"),
            os.environ.get("AIRFLOW__VAR__PRIVATE_SUBNET_2")],
    }}
logs_group = "/aws/ecs/" + os.environ.get("AIRFLOW__VAR__ADP_DBT_WORKER")
logs_stream_prefix = "ecs/" + os.environ.get("AIRFLOW__VAR__ADP_DBT_WORKER")
ecs_cluster = os.environ.get("AIRFLOW__VAR__ADP_DBT_WORKER")
stack = os.environ.get("AIRFLOW__VAR__ADP_ALLIANT_DATAPIPELINE")
"""
    return file_string

def generate_dbt_task_executor(file_string="", inc_glue=None):
    file_string += """

def overrided(models, tests, tests_vars):
    overrides = {
        "containerOverrides": [
            {
                "name": ecs_cluster,
                "environment": [
                    {
                        "name": "SF_WAREHOUSE",
                        "value": "TRANSFORM__DISTRIBUTION__ALL"
                    },
                    {
                        "name": "DBT_EXECUTE_RUN",
                        "value": "True"
                    },
                    {
                        "name": "DBT_EXECUTE_TEST",
                        "value": "True"
                    },
                    {
                        "name": "DBT_REPLACEMENT_MODELS",
                        "value": models
                    },
                    {
                        "name": "DBT_MODEL_TEST",
                        "value": tests
                    },
                    {
                        "name": "DBT_MODEL_TEST_VARS",
                        "value": tests_vars
                    }
                ]
            }
        ]
    }
    return overrides

def get_glue_job_full_name(glue_job_name):
    stack_string = os.environ.get("AIRFLOW__VAR__ADP_ALLIANT_DATAPIPELINE")
    glue_full_name = stack_string + glue_job_name

    return glue_full_name

    """

    return file_string

def generate_glue_function(file_string=""):
    file_string += """

def get_glue_job_full_name(glue_job_name):
    stack_string = os.environ.get("AIRFLOW__VAR__ADP_ALLIANT_DATAPIPELINE")
    glue_full_name = stack_string + glue_job_name

    return glue_full_name

    """
    return file_string

def dag_schedule(inc_schedule=None):
    if inc_schedule == True:
        dag_schedule_script = 'schedule_interval="0 6 * * 1,3,5"'
    else:
        dag_schedule_script = 'schedule_interval=None'
    return dag_schedule_script

# ToDo: fix schedule everywhere
def generate_dag_setup(file_string="", project_name="", schedule=None, dag_id=""):
    if schedule == True:
        dag_schedule_script = '"0 6 * * 1,3,5"'
    else:
        dag_schedule_script = 'None'
    file_string += f"""
with DAG(
    dag_id="{dag_id}",
    tags=["Alliant"],
    default_args=default_args,
    schedule_interval={dag_schedule_script},
    # dagrun_timeout=timedelta(minutes=10),
    catchup=False
) as dag:
"""
    return file_string

def generate_tasks(file_string="", manifest={}, inc_glue=None, inc_distribution=None):
    dbt_tasks = {}
    glue_task_group = GLUE_TASK_GROUP
    for node in manifest["nodes"].keys():
        if node.split(".")[0] == "model":
            if inc_distribution:
                # Define values for model/test name/path
                node_values = manifest["nodes"][node]
                node_name = clean_name(f"{node_values['name']}")
                if node_values["fqn"][1] == "examples":
                    continue

                has_test = False
                for child in manifest["child_map"][node]:
                    if "test" in child:
                        # it has a test so add the test to the call
                        has_test = True
                        break
                dbt_tasks[node_name] = make_dbt_task(node_name, has_test)

                # lets add a glue task if is a raw table (as we know there is a 1-2-1)
                if node_values["fqn"][1] == "raw" and node_values["fqn"][2] == "alliant_ppl_prod" and inc_glue:
                    table_name = node_values["config"]["alias"]
                    if table_name != "streams_validation_test":
                        glue_task = make_glue_task(table_name)
                        glue_task_group += f"\n        {glue_task}"
            else:
                if node not in DISTRIBUTION_MODELS:
                    # Define values for model/test name/path
                    node_values = manifest["nodes"][node]
                    node_name = clean_name(f"{node_values['name']}")
                    if node_values["fqn"][1] == "examples":
                        continue

                    has_test = False
                    for child in manifest["child_map"][node]:
                        if "test" in child:
                            # it has a test so add the test to the call
                            has_test = True
                            break
                    dbt_tasks[node_name] = make_dbt_task(node_name, has_test)

                    # lets add a glue task if is a raw table (as we know there is a 1-2-1)
                    if node_values["fqn"][1] == "raw" and node_values["fqn"][2] == "alliant_ppl_prod" and inc_glue:
                        table_name = node_values["config"]["alias"]
                        if table_name != "streams_validation_test":
                            glue_task = make_glue_task(table_name)
                            glue_task_group += f"\n        {glue_task}"

    dbt_tag_test = make_dbt_tag_test_relationships()

    for task_name, task_value in dbt_tasks.items():
        file_string += f"\n    {clean_task_name(task_name)} = {task_value}"
    file_string = file_string + dbt_tag_test

    if inc_glue:
        file_string += glue_task_group

    return file_string


def generate_task_dependency(file_string="", manifest={}, inc_glue=None, inc_distribution=None):
    for node in manifest["nodes"].keys():
        # if node != "model.adp.trns_vault_pre_aln_allocation":
        #     continue
        if node.split(".")[0] == "model":
            if inc_distribution:
                node_values = manifest["nodes"][node]
                node_name = clean_name(f"{node_values['name']}")
                if node_values["fqn"][1] == "examples":
                    continue
                for upstream_node in node_values["depends_on"]["nodes"]:
                    dependency = None
                    if "source." in upstream_node:
                        continue
                    upstream_node_values = manifest["nodes"][upstream_node]
                    upstream_node_name = clean_name(f"{upstream_node_values['name']}")
                    if "test." in upstream_node_name:
                        # if there's a test for the upstream node, set current model after it
                        dependency = None
                    else:
                        # else, set model after the upstream node
                        dependency = f"\n    {clean_task_name(upstream_node_name)} >> {clean_task_name(node_name)} "
                        file_string = add_dependency(file_string, dependency)

                if node_values["fqn"][1] == "raw" and node_values["fqn"][2] == "alliant_ppl_prod" and inc_glue:
                    table_name = node_values["config"]["alias"]
                    if table_name != "streams_validation_test":
                        glue_task = get_glue_airflow_task_name(table_name)
                        dependency = f"\n    {glue_task} >> stream_validation"
                        file_string = add_dependency(file_string, dependency)
                        stream_task = f"\n    stream_validation >> {clean_task_name(node_name)}"
                        file_string = add_dependency(file_string, stream_task)

                if node_values["fqn"][1] == "raw" and node_values["fqn"][2] == "alliant_ppl_prod" and inc_glue==None and node_name != "stream_validation":
                    stream_task = f"\n    stream_validation >> {clean_task_name(node_name)}"
                    file_string = add_dependency(file_string, stream_task)

                # set dependency for tag_test_relationship task, after pres layer tables
                if node_values["fqn"][1] == "pres" and node_values["fqn"][2] == "landing":
                    dependency = f"\n    {clean_task_name(node_name)} >> tag_test_relationships"
                    file_string = add_dependency(file_string, dependency)
            else:
                if node not in DISTRIBUTION_MODELS:
                    node_values = manifest["nodes"][node]
                    node_name = clean_name(f"{node_values['name']}")
                    if node_values["fqn"][1] == "examples":
                        continue
                    for upstream_node in node_values["depends_on"]["nodes"]:
                        dependency = None
                        if "source." in upstream_node:
                            continue
                        upstream_node_values = manifest["nodes"][upstream_node]
                        upstream_node_name = clean_name(f"{upstream_node_values['name']}")
                        if "test." in upstream_node_name:
                            # if there's a test for the upstream node, set current model after it
                            dependency = None
                        else:
                            # else, set model after the upstream node
                            dependency = f"\n    {clean_task_name(upstream_node_name)} >> {clean_task_name(node_name)} "
                            file_string = add_dependency(file_string, dependency)

                    if node_values["fqn"][1] == "raw" and node_values["fqn"][2] == "alliant_ppl_prod" and inc_glue:
                        table_name = node_values["config"]["alias"]
                        if table_name != "streams_validation_test":
                            glue_task = get_glue_airflow_task_name(table_name)
                            dependency = f"\n    {glue_task} >> stream_validation"
                            file_string = add_dependency(file_string, dependency)
                            stream_task = f"\n    stream_validation >> {clean_task_name(node_name)}"
                            file_string = add_dependency(file_string, stream_task)

                    if node_values["fqn"][1] == "raw" and node_values["fqn"][2] == "alliant_ppl_prod" and inc_glue==None and node_name != "stream_validation":
                        stream_task = f"\n    stream_validation >> {clean_task_name(node_name)}"
                        file_string = add_dependency(file_string, stream_task)

                    # set dependency for tag_test_relationship task, after pres layer tables
                    if node_values["fqn"][1] == "pres" and node_values["fqn"][2] == "landing":
                        dependency = f"\n    {clean_task_name(node_name)} >> tag_test_relationships"
                        file_string = add_dependency(file_string, dependency)

    return file_string


def main(args):
    '''
    dag_id = DAG ID
    dag_file_name = DAG Python file name
    schedule = Set to None or True. None for no schedule, True for "0 6 * * 1,3,5"
    inc_glue = Set to None or True
    inc_distribution = Set to None or True
    

    # DAG - Initialise:
    dag_id = "Alliant_master_dag_initialise"
    dag_file_name = f"{dag_id}.py"
    schedule = None
    inc_glue = None
    inc_distribution = True

    # DAG - Non-Distribution:
    dag_id = "Alliant_master_dag_non_distribution"
    dag_file_name = f"{dag_id}.py"
    schedule = True
    inc_glue = True
    inc_distribution = None

    # DAG - Distribution:
    dag_id = "Alliant_master_dag_distribution"
    dag_file_name = f"{dag_id}.py"
    schedule = None
    inc_glue = True
    inc_distribution = True
    '''

    # Define these to determine DAG attributes
    dag_id = "Alliant_master_dag_non_distribution"
    dag_file_name = f"{dag_id}.py"
    schedule = True
    inc_glue = True
    inc_distribution = None

    manifest = load_manifest()
    generated_dag_script = generate_imports(f"# DAG CREATED BY PYTHON SCRIPT -> {datetime.now()}\n")
    generated_dag_script = generate_default_configs(file_string=generated_dag_script, project_name="jaffle-shop")
    generated_dag_script = generate_dbt_task_executor(file_string=generated_dag_script, inc_glue=inc_glue)
    generated_dag_script = generate_dag_setup(file_string=generated_dag_script, project_name="jaffle-shop", schedule=schedule, dag_id=dag_id)
    generated_dag_script = generate_tasks(file_string=generated_dag_script, manifest=manifest, inc_glue=inc_glue, inc_distribution=inc_distribution)
    generated_dag_script = generate_task_dependency(file_string=generated_dag_script, manifest=manifest, inc_glue=inc_glue, inc_distribution=inc_distribution)
    with open(dag_file_name, "w") as f:
        f.write(generated_dag_script)
    with open(dag_file_name, 'r') as file :
        filedata = file.read()
        filedata = filedata.replace('models="stream_validation"', 'models=""')
    with open(dag_file_name, 'w') as file:
        file.write(filedata)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-schedule", help="""schedules the DAG with provided CRON schedule (required)""", required=False, default='1*1*1*', action='store', dest='schedule')
    parser.add_argument("-inc_glue", help="""includes glue tasks in the DAG""", required=False, default=True, action='store', dest='inc_glue')
    args = parser.parse_args()

    main(args)