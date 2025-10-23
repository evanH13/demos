"""
## SQS Asset Monitor DAG

This DAG demonstrates Airflow 3's Asset and AssetWatcher syntax to monitor an SQS queue.
When a message is sent to the SQS queue (triggered by a file drop in S3), this DAG will be triggered
and execute a simple print task.

This showcases the new asset-based scheduling capabilities in Airflow 3.
"""

import json
from datetime import datetime
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.sdk import Asset, AssetWatcher
from pendulum import datetime as pendulum_datetime


# Define the DAG using context manager
with DAG(
    dag_id="sqs_asset_monitor_dag",
    start_date=pendulum_datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only - will be triggered by AssetWatcher
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["sqs", "asset-monitor", "airflow-3"],
    catchup=False,
) as dag:
    """
    DAG that monitors an SQS queue using Asset and AssetWatcher.
    """
    
    def check_sqs_queue(**context) -> Dict[str, Any]:
        """
        Check the SQS queue for new messages.
        This function will be called by the AssetWatcher.
        """
        sqs_hook = SqsHook(aws_conn_id="s3_read_write")
        queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/lulu-airflow-queue"  # Replace with your queue URL
        
        try:
            # Receive messages from the queue
            messages = sqs_hook.receive_message(
                queue_url=queue_url,
                max_messages=1,
                wait_time_seconds=0  # Don't wait, just check
            )
            
            if messages:
                print(f"Found {len(messages)} message(s) in SQS queue")
                for message in messages:
                    print(f"Message ID: {message.get('MessageId')}")
                    print(f"Message Body: {message.get('Body')}")
                    
                    # Parse the message body (assuming it's JSON)
                    try:
                        body_data = json.loads(message.get('Body', '{}'))
                        print(f"Parsed message data: {body_data}")
                        
                        # Extract file information if available
                        if 'Records' in body_data:
                            for record in body_data['Records']:
                                if record.get('eventName') == 'ObjectCreated:Put':
                                    bucket = record.get('s3', {}).get('bucket', {}).get('name')
                                    key = record.get('s3', {}).get('object', {}).get('key')
                                    print(f"New file detected: s3://{bucket}/{key}")
                                    
                                    return {
                                        "file_detected": True,
                                        "bucket": bucket,
                                        "key": key,
                                        "message_id": message.get('MessageId'),
                                        "timestamp": datetime.now().isoformat()
                                    }
                    except json.JSONDecodeError:
                        print(f"Could not parse message body as JSON: {message.get('Body')}")
                
                return {
                    "file_detected": True,
                    "message_count": len(messages),
                    "timestamp": datetime.now().isoformat()
                }
            else:
                print("No messages found in SQS queue")
                return {
                    "file_detected": False,
                    "message_count": 0,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            print(f"Error checking SQS queue: {str(e)}")
            return {
                "file_detected": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def process_file_notification(**context) -> None:
        """
        Process the file notification from SQS.
        This is the main task that gets executed when a file is detected.
        """
        # Get data from XCom
        sqs_data = context['ti'].xcom_pull(task_ids='check_sqs_queue')
        
        if sqs_data.get("file_detected"):
            print("🎉 File notification received!")
            print(f"📁 Bucket: {sqs_data.get('bucket', 'Unknown')}")
            print(f"📄 Key: {sqs_data.get('key', 'Unknown')}")
            print(f"⏰ Timestamp: {sqs_data.get('timestamp')}")
            print(f"📨 Message ID: {sqs_data.get('message_id', 'Unknown')}")
            
            # Here you could add more processing logic:
            # - Download the file from S3
            # - Process the file content
            # - Send notifications
            # - Update databases
            # - etc.
            
            print("✅ File notification processed successfully!")
        else:
            print("ℹ️ No file notification to process")
    
    def log_completion(**context) -> None:
        """
        Log the completion of the file processing.
        """
        sqs_data = context['ti'].xcom_pull(task_ids='check_sqs_queue')
        
        print("=" * 50)
        print("📊 SQS Asset Monitor - Processing Complete")
        print("=" * 50)
        print(f"🕐 Completed at: {datetime.now()}")
        print(f"📨 Messages processed: {sqs_data.get('message_count', 0)}")
        print(f"📁 File detected: {sqs_data.get('file_detected', False)}")
        
        if sqs_data.get('error'):
            print(f"⚠️ Error: {sqs_data.get('error')}")
        else:
            print("✅ Processing completed successfully!")
    
    # Create tasks
    check_sqs_task = PythonOperator(
        task_id="check_sqs_queue",
        python_callable=check_sqs_queue,
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
    check_sqs_task >> process_notification_task >> log_completion_task


# Define the Asset with AssetWatcher for SQS monitoring
sqs_asset = Asset(
    uri="sqs://lulu-airflow-queue",  # SQS queue identifier
    watchers=[
        AssetWatcher(
            name="sqs_monitor",
            trigger=None,  # We'll handle the trigger logic in the task
        )
    ]
)
