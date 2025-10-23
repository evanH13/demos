# SQS Asset Monitor DAG

This DAG demonstrates Airflow 3's Asset and AssetWatcher syntax to monitor an SQS queue for file drop notifications.

## Features

- **Asset-based Monitoring**: Uses Airflow 3's new Asset and AssetWatcher syntax
- **SQS Queue Monitoring**: Monitors an SQS queue for new messages
- **File Drop Detection**: Detects when files are dropped in S3 (via SQS notifications)
- **Simple Processing**: Prints file information when detected

## Airflow 3 Asset System

### Asset Definition
```python
sqs_asset = Asset(
    uri="sqs://lulu-airflow-queue",
    description="SQS queue for file drop notifications"
)
```

### AssetWatcher Configuration
```python
sqs_watcher = AssetWatcher(
    asset=sqs_asset,
    dag=dag,
    task_id="check_sqs_queue",
    timeout=300,  # 5 minutes timeout
    check_interval=30,  # Check every 30 seconds
)
```

## Setup Requirements

### 1. SQS Queue Setup
Create an SQS queue in AWS:
```bash
aws sqs create-queue --queue-name lulu-airflow-queue
```

### 2. S3 Event Configuration
Configure your S3 bucket to send notifications to the SQS queue:
- Go to S3 bucket properties
- Add event notification
- Configure to send `s3:ObjectCreated:*` events to your SQS queue

### 3. Update Queue URL
Update the queue URL in the DAG:
```python
queue_url = "https://sqs.us-east-1.amazonaws.com/YOUR-ACCOUNT-ID/lulu-airflow-queue"
```

## How It Works

1. **File Drop**: When a file is dropped in the S3 bucket
2. **S3 Event**: S3 sends a notification to the SQS queue
3. **AssetWatcher**: Monitors the SQS queue for new messages
4. **DAG Trigger**: When a message is detected, the DAG is triggered
5. **Processing**: The DAG processes the file notification and prints details

## DAG Structure

### Tasks:
1. **`check_sqs_queue`**: Checks the SQS queue for new messages
2. **`process_file_notification`**: Processes the file notification
3. **`log_completion`**: Logs completion status

### AssetWatcher:
- **Monitors**: SQS queue for new messages
- **Triggers**: DAG when messages are detected
- **Timeout**: 5 minutes
- **Check Interval**: 30 seconds

## Usage

### Manual Testing
You can manually trigger the DAG to test the SQS monitoring:

1. Go to Airflow UI
2. Find `sqs_asset_monitor_dag`
3. Click "Trigger DAG"

### Automatic Triggering
The DAG will be automatically triggered when:
- A file is dropped in the monitored S3 bucket
- S3 sends a notification to the SQS queue
- AssetWatcher detects the new message

## Output Example

```
🎉 File notification received!
📁 Bucket: lulu-airflow-bucket
📄 Key: transformed_data_20250115_143022.json
⏰ Timestamp: 2025-01-15T14:30:22.123456
📨 Message ID: 12345678-1234-1234-1234-123456789012
✅ File notification processed successfully!
```

## Dependencies

- `apache-airflow-providers-amazon>=8.0.0` (for SQS operations)
- AWS SQS queue configured
- S3 bucket event notifications to SQS

## Customization

You can customize the DAG by:
- Changing the SQS queue URL
- Modifying the file processing logic
- Adding more sophisticated file handling
- Integrating with other services
- Adding error handling and retries
