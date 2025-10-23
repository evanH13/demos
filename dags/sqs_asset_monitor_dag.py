"""
## SQS Monitor DAG

This DAG monitors an SQS queue for messages using SqsSensor.
When a message is sent to the SQS queue (triggered by a file drop in S3), this DAG will process
the message and execute a simple print task.

This demonstrates SQS-based DAG triggering in Airflow.
"""

import json
from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from pendulum import datetime as pendulum_datetime


# Define the DAG using context manager
with DAG(
    dag_id="sqs_monitor_dag",
    start_date=pendulum_datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["sqs", "monitor", "sensor"],
    catchup=False,
) as dag:
    """
    DAG that monitors an SQS queue using SqsSensor.
    """
    
    
    def process_file_notification(**context) -> None:
        """
        Process the file notification from SQS.
        This is the main task that gets executed when a file is detected.
        """
        # Get the SQS message from the sensor
        sqs_messages = context['ti'].xcom_pull(task_id='wait_for_sqs_message')
        
        if sqs_messages:
            print("🎉 SQS message received!")
            print(f"📨 Number of messages: {len(sqs_messages)}")
            
            for message in sqs_messages:
                print(f"📨 Message ID: {message.get('MessageId')}")
                print(f"📄 Message Body: {message.get('Body')}")
                
                # Parse the message body (assuming it's JSON)
                try:
                    body_data = json.loads(message.get('Body', '{}'))
                    print(f"📊 Parsed message data: {body_data}")
                    
                    # Extract file information if available
                    if 'Records' in body_data:
                        for record in body_data['Records']:
                            if record.get('eventName') == 'ObjectCreated:Put':
                                bucket = record.get('s3', {}).get('bucket', {}).get('name')
                                key = record.get('s3', {}).get('object', {}).get('key')
                                print(f"📁 New file detected: s3://{bucket}/{key}")
                                
                                # Here you could add more processing logic:
                                # - Download the file from S3
                                # - Process the file content
                                # - Send notifications
                                # - Update databases
                                # - etc.
                                
                except json.JSONDecodeError:
                    print(f"⚠️ Could not parse message body as JSON: {message.get('Body')}")
            
            print("✅ File notification processed successfully!")
        else:
            print("ℹ️ No SQS messages to process")
    
    def log_completion(**context) -> None:
        """
        Log the completion of the file processing.
        """
        sqs_messages = context['ti'].xcom_pull(task_id='wait_for_sqs_message')
        
        print("=" * 50)
        print("📊 SQS Monitor - Processing Complete")
        print("=" * 50)
        print(f"🕐 Completed at: {datetime.now()}")
        print(f"📨 Messages processed: {len(sqs_messages) if sqs_messages else 0}")
        print(f"📁 File detected: {bool(sqs_messages)}")
        
        if sqs_messages:
            print("✅ Processing completed successfully!")
        else:
            print("ℹ️ No messages to process")
    
    # Create tasks using SqsSensor for proper SQS monitoring
    sqs_sensor = SqsSensor(
        task_id="wait_for_sqs_message",
        sqs_queue="https://sqs.us-east-1.amazonaws.com/285860431378/lulu-airflow-queue",
        aws_conn_id="s3_read_write",
        max_messages=1,
        wait_time_seconds=0,  # Don't wait, just check
        poke_interval=30,  # Check every 30 seconds
        timeout=300,  # 5 minutes timeout
    )
    
    process_notification_task = PythonOperator(
        task_id="process_file_notification",
        python_callable=process_file_notification,
    )
    
    log_completion_task = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
    )
    
    # Define the task flow
    sqs_sensor >> process_notification_task >> log_completion_task
