"""
## Snowflake Connection Test DAG

This DAG provides a simple way to test a Snowflake connection in Astro.
It performs basic connectivity checks and displays connection information
to verify that the Snowflake connection is working properly.

The DAG includes:
- Connection validation
- Basic query execution
- Connection details display
- Error handling for connection issues

This is perfect for PoC testing to ensure Snowflake connectivity is working.
"""

from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pendulum import datetime as pendulum_datetime


@dag(
    dag_id="snowflake_connection_test",
    start_date=pendulum_datetime(2025, 1, 1),
    schedule="@once",  # Run once for testing
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["snowflake", "connection-test", "poc"],
    catchup=False,
)
def snowflake_connection_test():
    """
    DAG to test Snowflake connection and perform basic connectivity checks.
    """
    
    @task
    def test_snowflake_connection(**context) -> dict:
        """
        Test the Snowflake connection by executing a simple query.
        Returns connection details and query results.
        """
        try:
            # Initialize Snowflake hook
            snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
            
            # Get connection details
            conn = snowflake_hook.get_connection("snowflake_default")
            
            # Test connection with the most basic query possible
            result = snowflake_hook.get_first("SELECT 1 as test")
            
            if result:
                return {
                    "status": "success",
                    "connection_id": conn.conn_id,
                    "host": conn.host
                }
            else:
                return {"status": "error", "message": "Query returned no results"}
                
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    @task
    def display_connection_summary(test_results: dict) -> None:
        """
        Display a summary of the connection test results.
        """
        if test_results["status"] == "success":
            print("Snowflake Connection Test: SUCCESS")
            print(f"Connection ID: {test_results.get('connection_id', 'N/A')}")
            print(f"Host: {test_results.get('host', 'N/A')}")
        else:
            print("Snowflake Connection Test: FAILED")
            print(f"Error: {test_results.get('message', 'Unknown error')}")
    
    # Define the task flow
    test_results = test_snowflake_connection()
    display_connection_summary(test_results)


# Instantiate the DAG
snowflake_connection_test()
