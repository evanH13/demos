"""
## SQS Asset Monitor DAG

This DAG demonstrates Airflow 3's Asset and AssetWatcher system to monitor an SQS queue.
When a message is sent to the SQS queue (triggered by a file drop in S3), this DAG will be triggered
and execute a simple print task.

This showcases the new asset-based scheduling capabilities in Airflow 3.
"""

import json
from datetime import datetime
from typing import Any, Dict

from airflow.sdk import Asset, AssetWatcher, dag, task
from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger
from pendulum import datetime as pendulum_datetime


# Define the SQS queue URL
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/285860431378/lulu-airflow-queue"

# Define the SQS trigger
sqs_trigger = SqsSensorTrigger(
    sqs_queue=SQS_QUEUE_URL,
    aws_conn_id="s3_read_write",
    waiter_delay=10,  # Time in seconds between polls
)

# Define the asset with an AssetWatcher
sqs_asset = Asset(
    "sqs://lulu-airflow-queue",
    watchers=[AssetWatcher(name="sqs_watcher", trigger=sqs_trigger)],
)

# Define the DAG using AssetWatcher
@dag(
    dag_id="sqs_asset_monitor_dag",
    start_date=pendulum_datetime(2025, 1, 1),
    schedule=[sqs_asset],  # Schedule based on asset changes
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 1},
    tags=["sqs", "asset-monitor", "airflow-3"],
    catchup=False,
)
def sqs_asset_monitor_dag():
    """
    DAG that monitors an SQS queue using Airflow 3's Asset and AssetWatcher system.
    """
    
    @task
    def process_message(**context) -> None:
        """
        Process the SQS message that triggered the DAG.
        This task will be triggered when the AssetWatcher detects new SQS messages.
        """
        print("🎉 AssetWatcher triggered! Processing SQS message...")
        
        # Extract the triggering asset events from the context
        triggering_asset_events = context.get("triggering_asset_events", {})
        
        if sqs_asset in triggering_asset_events:
            events = triggering_asset_events[sqs_asset]
            print(f"📨 Found {len(events)} asset event(s)")
            
            for event in events:
                # Get the message from the TriggerEvent payload
                try:
                    message_batch = event.extra["payload"]["message_batch"]
                    for message in message_batch:
                        message_body = message["Body"]
                        print(f"📄 Message Body: {message_body}")
                        
                        # Parse the message body (assuming it's JSON)
                        try:
                            body_data = json.loads(message_body)
                            print(f"📊 Parsed message data: {body_data}")
                            
                            # Extract file information if available
                            if 'Records' in body_data:
                                for record in body_data['Records']:
                                    if record.get('eventName') == 'ObjectCreated:Put':
                                        bucket = record.get('s3', {}).get('bucket', {}).get('name')
                                        key = record.get('s3', {}).get('object', {}).get('key')
                                        print(f"📁 New file detected: s3://{bucket}/{key}")
                                        
                                        print("🎉 File notification received!")
                                        print(f"📁 Bucket: {bucket}")
                                        print(f"📄 Key: {key}")
                                        print(f"⏰ Timestamp: {datetime.now()}")
                                        
                                        # Here you could add more processing logic:
                                        # - Download the file from S3
                                        # - Process the file content
                                        # - Send notifications
                                        # - Update databases
                                        # - etc.
                                        
                                        print("✅ File notification processed successfully!")
                                        
                        except json.JSONDecodeError:
                            print(f"⚠️ Could not parse message body as JSON: {message_body}")
                            
                except KeyError as e:
                    print(f"⚠️ Could not extract message from event: {e}")
                    print(f"Event structure: {event}")
        else:
            print("ℹ️ No asset events found in context")
    
    @task
    def log_completion(**context) -> None:
        """
        Log the completion of the message processing.
        """
        print("=" * 50)
        print("📊 SQS Asset Monitor - Processing Complete")
        print("=" * 50)
        print(f"🕐 Completed at: {datetime.now()}")
        print("✅ SQS message processing completed successfully!")
    
    # Define the task flow
    process_message()
    log_completion()


# Instantiate the DAG
sqs_asset_monitor_dag()
