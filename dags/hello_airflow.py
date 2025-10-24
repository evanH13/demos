from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Run manually from the UI
    catchup=False,
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Starting the DAG...'"
    )

    hello_dan = BashOperator(
        task_id="hello_dan",
        bash_command="echo 'hi dan'"
    )

    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello, Airflow!'"
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo 'All done!'"
    )

    start >> hello_dan >> hello >> end