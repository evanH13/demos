"""
## SQS Asset Monitor DAG

This DAG demonstrates Airflow 3's Asset and AssetWatcher system.
For demonstration purposes, it uses a file-based trigger to simulate SQS monitoring.
In production, you would use a proper SQS trigger.

This showcases the new asset-based scheduling capabilities in Airflow 3.
"""

import json
from datetime import datetime
from typing import Any, Dict

from airflow.sdk import DAG, Asset, AssetWatcher, task
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from pendulum import datetime as pendulum_datetime


# Define a file-based trigger for demonstration
# This simulates SQS monitoring by watching for a trigger file
file_trigger = FileDeleteTrigger(filepath="/tmp/sqs_trigger_file")

sqs_asset = Asset(
    "sqs://lulu-airflow-queue",
    watchers=[
        AssetWatcher(
            name="sqs_monitor",
            trigger=file_trigger
        )
    ]
)

# Define the DAG using AssetWatcher
with DAG(
    dag_id="sqs_asset_monitor_dag",
    start_date=pendulum_datetime(2025, 1, 1),
    schedule=[sqs_asset],  # Schedule based on asset changes
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["sqs", "asset-monitor", "airflow-3"],
    catchup=False,
):
    """
    DAG that monitors an SQS queue using Airflow 3's Asset and AssetWatcher system.
    """
    
    @task
    def check_sqs_queue(**context) -> Dict[str, Any]:
        """
        Check the SQS queue for new messages and return the results.
        This task will be triggered when the AssetWatcher detects the trigger file deletion.
        """
        print("🎉 AssetWatcher triggered! Checking SQS queue...")
        
        sqs_hook = SqsHook(aws_conn_id="s3_read_write")
        queue_url = "https://sqs.us-east-1.amazonaws.com/285860431378/lulu-airflow-queue"
        
        try:
            # Receive messages from the queue
            messages = sqs_hook.receive_message(
                queue_url=queue_url,
                max_messages=1,
                wait_time_seconds=0  # Don't wait, just check
            )
            
            if messages:
                print(f"🎉 Found {len(messages)} message(s) in SQS queue")
                for message in messages:
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
                                    
                                    return {
                                        "file_detected": True,
                                        "bucket": bucket,
                                        "key": key,
                                        "message_id": message.get('MessageId'),
                                        "timestamp": datetime.now().isoformat()
                                    }
                    except json.JSONDecodeError:
                        print(f"⚠️ Could not parse message body as JSON: {message.get('Body')}")
                
                return {
                    "file_detected": True,
                    "message_count": len(messages),
                    "timestamp": datetime.now().isoformat()
                }
            else:
                print("ℹ️ No messages found in SQS queue")
                return {
                    "file_detected": False,
                    "message_count": 0,
                    "timestamp": datetime.now().isoformat()
                }
                
        except Exception as e:
            print(f"❌ Error checking SQS queue: {str(e)}")
            return {
                "file_detected": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    @task
    def process_file_notification(sqs_data: Dict[str, Any]) -> None:
        """
        Process the file notification from SQS.
        This is the main task that gets executed when a file is detected.
        """
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
    
    @task
    def log_completion(sqs_data: Dict[str, Any]) -> None:
        """
        Log the completion of the file processing.
        """
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
    
    # Define the task flow using TaskFlow API
    sqs_data = check_sqs_queue()
    process_file_notification(sqs_data)
    log_completion(sqs_data)
