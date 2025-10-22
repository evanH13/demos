from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode
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

jaffle_dbt = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        execution_mode=ExecutionMode.LOCAL
    ),
    render_config=RenderConfig(
        select=["staging.*","marts.*"],
        test_behavior="after_all"
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="jaffle_shop_cosmos_snowflake_fieldengineer",
)
