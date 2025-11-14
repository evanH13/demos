"""
## EMR Spark Job DAG

This DAG demonstrates a complete EMR workflow:
1. Creates an EMR cluster
2. Adds a Spark step to the cluster (runs a simple Spark job)
3. Waits for the step to complete
4. Terminates the cluster

The DAG uses the AWS connection "aws_se_demo" which is configured with workload identity
to assume a role. This allows secure access to AWS services without storing long-lived credentials.

The Spark step runs a simple Pi calculation as a demonstration. In production, you would
replace this with your actual Spark application (e.g., data processing, ETL jobs, etc.).

**Configuration:**
- AWS Connection ID: aws_se_demo (configured with workload identity)
- EMR Release: emr-6.15.0 (latest stable release)
- Instance Type: m5.xlarge (can be customized)
- Instance Count: 2 (1 master + 1 core node)
- Auto-terminate: True (cluster terminates after steps complete)
"""

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from pendulum import datetime as pendulum_datetime

# Default EMR configuration
EMR_CLUSTER_CONFIG = {
    "Name": "airflow-emr-cluster",
    "ReleaseLabel": "emr-6.15.0",
    "Applications": [
        {"Name": "Spark"},
        {"Name": "Hadoop"},
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,  # Auto-terminate when done
        "TerminationProtected": False,
        "Ec2SubnetId": None,  # Use default VPC subnet, customize if needed
    },
    "JobFlowRole": "EMR_DefaultRole",  # Default EMR role
    "ServiceRole": "EMR_DefaultRole",  # Default EMR service role
    # LogUri is optional - remove or update with your S3 bucket path if you want EMR logs
    # "LogUri": "s3://your-bucket/emr-logs/",
    "BootstrapActions": [],
}

# Spark step configuration - runs a simple Pi calculation
SPARK_STEP = {
    "Name": "Spark Pi Calculation",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            "--class",
            "org.apache.spark.examples.SparkPi",
            "s3://us-east-1.elasticmapreduce.samples/cloudfront/code/spark-examples.jar",
            "10",  # Number of slices for Pi calculation
        ],
    },
}

# Define the DAG
with DAG(
    dag_id="emr_spark_job",
    start_date=pendulum_datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only - EMR clusters are expensive
    doc_md=__doc__,
    default_args={
        "owner": "Astro",
        "retries": 1,
        "aws_conn_id": "s3_read_write",  # AWS connection with workload identity
    },
    tags=["emr", "spark", "aws", "data-processing"],
    catchup=False,
) as dag:
    """
    DAG that creates an EMR cluster, runs a Spark job, and terminates the cluster.
    """

    # Step 1: Create EMR cluster
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_CLUSTER_CONFIG,
        aws_conn_id="aws_se_demo",
    )

    # Step 2: Add Spark step to the cluster
    add_spark_step = EmrAddStepsOperator(
        task_id="add_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        steps=[SPARK_STEP],
        aws_conn_id="aws_se_demo",
    )

    # Step 3: Wait for the Spark step to complete
    wait_for_step = EmrStepSensor(
        task_id="wait_for_spark_step",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
        aws_conn_id="aws_se_demo",
        poke_interval=30,  # Check every 30 seconds
        timeout=3600,  # Timeout after 1 hour
    )

    # Step 4: Terminate the EMR cluster
    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_se_demo",
    )

    # Define the task flow
    create_emr_cluster >> add_spark_step >> wait_for_step >> terminate_emr_cluster

