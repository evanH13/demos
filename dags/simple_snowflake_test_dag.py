"""
## Simple Snowflake Connection Test DAG

This DAG performs a simple connectivity test to verify that Airflow can reach Snowflake.
It executes a basic query to confirm the connection is working properly.

This is useful for:
- Verifying Snowflake connection configuration
- Testing network connectivity
- Validating credentials
- Quick health checks
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime as pendulum_datetime


def test_snowflake_connection(**context):
    """
    Test Snowflake connection by executing a simple query.
    This verifies that:
    1. The connection can be established
    2. Credentials are valid
    3. The service can reach Snowflake
    """
    # Default connection ID - can be overridden via DAG params
    conn_id = "snowflake"
    try:
        # Initialize Snowflake hook
        snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)
        
        # Get connection details (without exposing sensitive info)
        conn = snowflake_hook.get_connection(conn_id)
        print(f"Connection ID: {conn.conn_id}")
        print(f"Host: {conn.host}")
        print(f"Schema: {conn.schema}")
        
        # Execute the simplest possible query to test connectivity
        print("Executing test query: SELECT 1")
        result = snowflake_hook.get_first("SELECT 1 as test_value")
        
        if result:
            print(f"Query executed successfully! Result: {result}")
            
            # Also get Snowflake version to confirm we're actually talking to Snowflake
            version_result = snowflake_hook.get_first("SELECT CURRENT_VERSION() as version")
            if version_result:
                print(f"Snowflake version: {version_result[0]}")
            
            print("\nSnowflake connection test: SUCCESS")
            print("The service can successfully reach Snowflake.")
            return {"status": "success", "message": "Connection test passed"}
        else:
            error_msg = "Query executed but returned no results"
            print(f"ERROR: {error_msg}")
            raise Exception(error_msg)
            
    except Exception as e:
        error_msg = f"Snowflake connection test failed: {str(e)}"
        print(f"\nERROR: {error_msg}")
        raise Exception(error_msg)


# Define the DAG
with DAG(
    dag_id="simple_snowflake_test",
    start_date=pendulum_datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only - perfect for testing
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["snowflake", "connection-test", "health-check"],
    catchup=False,
    params={"snowflake_conn_id": "snowflake"},  # Configurable connection ID
) as dag:
    """
    DAG that tests Snowflake connectivity with a simple query.
    """
    
    test_connection_task = PythonOperator(
        task_id="test_snowflake_connection",
        python_callable=test_snowflake_connection,
    )

