"""
## JSON Transform DAG

This DAG transforms a static JSON document into XML, JSON, or CSV format
based on a runtime configuration parameter and uploads the result to S3.

The DAG accepts a runtime configuration parameter 'output_format' that can be:
- 'xml': Converts JSON to XML format
- 'json': Keeps the original JSON format (with optional formatting)
- 'csv': Converts JSON to CSV format (flattens nested structures)

The transformed data is then uploaded to an S3 bucket using the 's3_read_write' connection.
"""

import json
import csv
import xml.etree.ElementTree as ET
from io import StringIO
from typing import Any, Dict, List
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime as pendulum_datetime


# Sample JSON data - in a real scenario, this could be loaded from a file or external source
SAMPLE_JSON_DATA = {
    "employees": [
        {
            "id": 1,
            "name": "John Doe",
            "department": "Engineering",
            "salary": 75000,
            "skills": ["Python", "SQL", "Airflow"],
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip": "10001"
            }
        },
        {
            "id": 2,
            "name": "Jane Smith",
            "department": "Marketing",
            "salary": 65000,
            "skills": ["Digital Marketing", "Analytics", "SEO"],
            "address": {
                "street": "456 Oak Ave",
                "city": "San Francisco",
                "state": "CA",
                "zip": "94102"
            }
        },
        {
            "id": 3,
            "name": "Bob Johnson",
            "department": "Sales",
            "salary": 70000,
            "skills": ["CRM", "Lead Generation", "Negotiation"],
            "address": {
                "street": "789 Pine St",
                "city": "Chicago",
                "state": "IL",
                "zip": "60601"
            }
        }
    ],
    "company": {
        "name": "TechCorp Inc",
        "founded": 2010,
        "headquarters": "New York, NY"
    }
}


# Define the DAG using context manager
with DAG(
    dag_id="json_transform_dag",
    start_date=pendulum_datetime(2025, 1, 1),
    schedule=None,  # Manual trigger only
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 2},
    tags=["json-transform", "s3", "data-processing"],
    catchup=False,
) as dag:
    """
    DAG that transforms JSON data based on runtime configuration and uploads to S3.
    """
    
    def get_runtime_config(**context) -> Dict[str, Any]:
        """
        Extract runtime configuration from DAG run configuration.
        Defaults to 'json' format if not specified.
        """
        config = context.get("dag_run", {}).get("conf", {})
        output_format = config.get("output_format", "json")
        
        # Validate output format
        valid_formats = ["json", "xml", "csv"]
        if output_format not in valid_formats:
            raise ValueError(f"Invalid output_format '{output_format}'. Must be one of: {valid_formats}")
        
        print(f"Runtime configuration: output_format = {output_format}")
        return {"output_format": output_format}
    
    def load_json_data() -> Dict[str, Any]:
        """
        Load the static JSON data.
        In a real scenario, this could load from a file, database, or API.
        """
        print(f"Loaded JSON data with {len(SAMPLE_JSON_DATA['employees'])} employees")
        return SAMPLE_JSON_DATA
    
    def transform_to_xml(json_data: Dict[str, Any]) -> str:
        """
        Transform JSON data to XML format.
        """
        def dict_to_xml(data: Any, root_name: str = "root") -> ET.Element:
            root = ET.Element(root_name)
            
            if isinstance(data, dict):
                for key, value in data.items():
                    if isinstance(value, (dict, list)):
                        child = dict_to_xml(value, key)
                        root.append(child)
                    else:
                        child = ET.SubElement(root, key)
                        child.text = str(value)
            elif isinstance(data, list):
                for i, item in enumerate(data):
                    child = dict_to_xml(item, f"item_{i}")
                    root.append(child)
            else:
                root.text = str(data)
            
            return root
        
        root = dict_to_xml(json_data, "data")
        xml_str = ET.tostring(root, encoding='unicode', method='xml')
        
        # Pretty print the XML
        import xml.dom.minidom
        dom = xml.dom.minidom.parseString(xml_str)
        pretty_xml = dom.toprettyxml(indent="  ")
        
        print(f"Transformed JSON to XML ({len(pretty_xml)} characters)")
        return pretty_xml
    
    def transform_to_json(json_data: Dict[str, Any]) -> str:
        """
        Transform JSON data to formatted JSON string.
        """
        formatted_json = json.dumps(json_data, indent=2, ensure_ascii=False)
        print(f"Formatted JSON ({len(formatted_json)} characters)")
        return formatted_json
    
    def transform_to_csv(json_data: Dict[str, Any]) -> str:
        """
        Transform JSON data to CSV format by flattening nested structures.
        """
        def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
            """
            Flatten a nested dictionary.
            """
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                elif isinstance(v, list):
                    # Convert list to string representation
                    items.append((new_key, str(v)))
                else:
                    items.append((new_key, v))
            return dict(items)
        
        # Extract employees data and flatten each record
        employees = json_data.get("employees", [])
        if not employees:
            return "No employee data found"
        
        # Flatten each employee record
        flattened_employees = []
        for emp in employees:
            flattened_emp = flatten_dict(emp)
            flattened_employees.append(flattened_emp)
        
        # Create CSV content
        output = StringIO()
        if flattened_employees:
            fieldnames = flattened_employees[0].keys()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flattened_employees)
        
        csv_content = output.getvalue()
        print(f"Transformed JSON to CSV ({len(csv_content)} characters)")
        return csv_content
    
    def transform_data(**context) -> str:
        """
        Transform JSON data based on the specified output format.
        """
        # Get data from XCom
        json_data = context['ti'].xcom_pull(task_ids='load_json_data')
        config = context['ti'].xcom_pull(task_ids='get_runtime_config')
        
        output_format = config["output_format"]
        
        if output_format == "xml":
            return transform_to_xml(json_data)
        elif output_format == "csv":
            return transform_to_csv(json_data)
        else:  # json
            return transform_to_json(json_data)
    
    def upload_to_s3(**context) -> str:
        """
        Upload the transformed data to S3. Kick.
        """
        # Get data from XCom
        transformed_data = context['ti'].xcom_pull(task_ids='transform_data')
        config = context['ti'].xcom_pull(task_ids='get_runtime_config')
        
        output_format = config["output_format"]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate filename based on format
        file_extension = {
            "json": "json",
            "xml": "xml", 
            "csv": "csv"
        }[output_format]
        
        filename = f"transformed_data_{timestamp}.{file_extension}"
        
        # Upload to S3
        s3_hook = S3Hook(aws_conn_id="s3_read_write")
        bucket_name = "lulu-airflow-bucket"  # Replace with actual bucket name
        
        try:
            s3_hook.load_string(
                string_data=transformed_data,
                key=filename,
                bucket_name=bucket_name,
                replace=True
            )
            
            s3_url = f"s3://{bucket_name}/{filename}"
            print(f"Successfully uploaded {filename} to S3: {s3_url}")
            return s3_url
            
        except Exception as e:
            print(f"Error uploading to S3: {str(e)}")
            raise
    
    def log_completion(**context) -> None:
        """
        Log the completion of the transformation and upload.
        """
        # Get data from XCom
        s3_url = context['ti'].xcom_pull(task_ids='upload_to_s3')
        config = context['ti'].xcom_pull(task_ids='get_runtime_config')
        
        output_format = config["output_format"]
        print(f"✅ Transformation completed successfully!")
        print(f"📄 Output format: {output_format.upper()}")
        print(f"📦 S3 location: {s3_url}")
        print(f"⏰ Completed at: {datetime.now()}")
    
    # Create tasks using PythonOperator
    get_config_task = PythonOperator(
        task_id="get_runtime_config",
        python_callable=get_runtime_config,
    )
    
    load_data_task = PythonOperator(
        task_id="load_json_data",
        python_callable=load_json_data,
    )
    
    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )
    
    upload_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )
    
    log_completion_task = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
    )
    
    # Define the task flow
    get_config_task >> load_data_task >> transform_data_task >> upload_s3_task >> log_completion_task
