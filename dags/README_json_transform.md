# JSON Transform DAG

This DAG transforms a static JSON document into XML, JSON, or CSV format based on a runtime configuration parameter and uploads the result to S3.

## Features

- **Runtime Configuration**: Accepts `output_format` parameter to specify transformation type
- **Multiple Output Formats**: Supports JSON, XML, and CSV transformations
- **S3 Integration**: Uploads transformed data to S3 using the `s3_read_write` connection
- **Error Handling**: Includes validation and error handling for all operations

## Usage

### Triggering the DAG

The DAG is set to manual trigger only (`schedule=None`). To run it:

1. Go to the Airflow UI
2. Find the `json_transform_dag` DAG
3. Click "Trigger DAG w/ Config"
4. Provide the configuration JSON:

```json
{
  "output_format": "json"
}
```

### Configuration Options

The `output_format` parameter accepts three values:

- **`"json"`**: Formats the JSON with proper indentation (default)
- **`"xml"`**: Converts JSON to XML format with proper structure
- **`"csv"`**: Flattens nested JSON structures into CSV format

### Example Configurations

#### JSON Output (Default)
```json
{
  "output_format": "json"
}
```

#### XML Output
```json
{
  "output_format": "xml"
}
```

#### CSV Output
```json
{
  "output_format": "csv"
}
```

## DAG Structure

The DAG consists of the following tasks:

1. **`get_runtime_config`**: Extracts and validates runtime configuration
2. **`load_json_data`**: Loads the static JSON data
3. **`transform_data`**: Transforms data based on the specified format
4. **`upload_to_s3`**: Uploads the transformed data to S3
5. **`log_completion`**: Logs completion status

## S3 Configuration

Before running the DAG, make sure to:

1. **Update the bucket name**: In the `upload_to_s3` task, replace `"your-bucket-name"` with your actual S3 bucket name
2. **Verify S3 connection**: Ensure the `s3_read_write` connection is properly configured in Airflow

## Sample Data

The DAG uses sample employee data with the following structure:

```json
{
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
    }
  ],
  "company": {
    "name": "TechCorp Inc",
    "founded": 2010,
    "headquarters": "New York, NY"
  }
}
```

## Output Files

The DAG generates files with the following naming convention:
- `transformed_data_YYYYMMDD_HHMMSS.json`
- `transformed_data_YYYYMMDD_HHMMSS.xml`
- `transformed_data_YYYYMMDD_HHMMSS.csv`

## Dependencies

The DAG requires the following Python packages:
- `apache-airflow-providers-amazon>=8.0.0` (for S3 operations)
- Standard library modules: `json`, `csv`, `xml.etree.ElementTree`

## Error Handling

The DAG includes error handling for:
- Invalid output format specification
- S3 upload failures
- Data transformation errors

## Customization

To use your own JSON data:
1. Replace the `SAMPLE_JSON_DATA` constant with your data
2. Modify the transformation logic if needed for your specific data structure
3. Update the S3 bucket name and file naming convention as required
