from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from cosmos import ProjectConfig, ProfileConfig, RenderConfig
from cosmos import DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

DBT_PROJECT_PATH = "/usr/local/airflow/dags/dbt_jaffle_cosmos"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

profile_config = ProfileConfig(
    profile_name="jaffle_shop_cosmos",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake",
        profile_args={
            "database": "SANDBOX",
            "schema": "FIELDENGINEER",
            "warehouse": "TINY_ROBOTS",
            "role": "FIELDENGINEER",
        },
    ),
)

def _dummy_extract():
    print("Pretend to extract/land raw files…")

def _post_transform_notify():
    print("Notifying downstream system that dbt models are fresh.")

with DAG(
    dag_id="cosmos_dbt_taskgroup_split_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=_dummy_extract)

    # First TaskGroup: staging models only
    dbt_staging = DbtTaskGroup(
        group_id="dbt_staging",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["staging.*"],
            test_behavior="after_all",  # run tests after the staging group
        ),
    )

    # Second TaskGroup: marts models only
    dbt_marts = DbtTaskGroup(
        group_id="dbt_marts",
        project_config=project_config,
        profile_config=profile_config,
        render_config=RenderConfig(
            select=["marts.*"],
            test_behavior="after_all",  # run tests after the marts group
        ),
    )

    dq_gate = EmptyOperator(task_id="data_quality_gate")
    notify = PythonOperator(task_id="notify_downstream", python_callable=_post_transform_notify)

    # Wiring: extract -> staging -> marts -> dq_gate -> notify
    extract >> dbt_staging >> dbt_marts >> dq_gate >> notify
