# Snowflake Connection Test DAG

## Overview

This DAG provides a simple way to test a Snowflake connection in Astro for PoC (Proof of Concept) purposes. It performs basic connectivity checks and displays connection information to verify that the Snowflake connection is working properly.

## Features

- **Connection Validation**: Tests the Snowflake connection using the configured connection ID
- **Basic Query Execution**: Runs simple queries to verify database access
- **Connection Details Display**: Shows connection information including host, database, schema, warehouse, and role
- **Error Handling**: Provides clear error messages if connection fails
- **Comprehensive Testing**: Tests version, database, schema, and warehouse access

## Prerequisites

1. **Snowflake Connection**: Ensure you have a Snowflake connection configured in Astro with the connection ID `snowflake_default`
2. **Required Packages**: The DAG uses the Snowflake provider which should be included in your requirements

## Usage

### For PoC Testing

1. **Configure Connection**: Make sure your Snowflake connection is set up in Astro with the connection ID `snowflake_default`
2. **Run the DAG**: The DAG is scheduled to run `@once` by default, so it will execute immediately when enabled
3. **Check Results**: Review the task logs to see connection details and test results

### Connection Configuration

The DAG expects a Snowflake connection with the following details:
- **Connection ID**: `snowflake_default`
- **Connection Type**: Snowflake
- **Host**: Your Snowflake account URL
- **Login**: Your Snowflake username
- **Password**: Your Snowflake password
- **Extra**: JSON containing:
  ```json
  {
    "database": "your_database",
    "warehouse": "your_warehouse", 
    "role": "your_role"
  }
  ```

## DAG Structure

### Tasks

1. **`test_snowflake_connection`**: 
   - Tests the Snowflake connection
   - Executes basic queries to verify access
   - Returns connection details and test results

2. **`display_connection_summary`**:
   - Displays a comprehensive summary of the connection test
   - Shows success/failure status with detailed information

### Schedule

- **Default**: `@once` (runs once for testing)
- **For Regular Testing**: Change to `@daily` or `@hourly` as needed

## Expected Output

### Successful Connection
```
🔗 Testing Snowflake Connection...
📊 Connection ID: snowflake_default
🏠 Host: your-account.snowflakecomputing.com
👤 Login: your_username
🗄️ Schema: PUBLIC
🏢 Database: your_database
🏭 Warehouse: your_warehouse
🔧 Role: your_role

🔍 Executing test query...
✅ Connection successful!
📋 Snowflake Version: 8.45.0
⏰ Current Timestamp: 2025-01-XX XX:XX:XX

🗄️ Testing database access...
📊 Current Database: your_database
📁 Current Schema: PUBLIC

🏭 Testing warehouse access...
🏭 Current Warehouse: your_warehouse

============================================================
📊 SNOWFLAKE CONNECTION TEST SUMMARY
============================================================
✅ Connection Status: SUCCESS
🔗 Connection ID: snowflake_default
🏠 Host: your-account.snowflakecomputing.com
📋 Snowflake Version: 8.45.0
🗄️ Database: your_database
📁 Schema: PUBLIC
🏭 Warehouse: your_warehouse
⏰ Test Timestamp: 2025-01-XX XX:XX:XX

🎉 Snowflake connection is working properly!
✅ Ready for data operations!
============================================================
```

### Failed Connection
```
❌ Connection test failed: Authentication failed for user 'username'

============================================================
📊 SNOWFLAKE CONNECTION TEST SUMMARY
============================================================
❌ Connection Status: FAILED
🚨 Error: Authentication failed for user 'username'

🔧 Please check your Snowflake connection configuration:
   - Verify connection ID is correct
   - Check host, username, and password
   - Ensure database, schema, and warehouse are accessible
   - Verify network connectivity
============================================================
```

## Troubleshooting

### Common Issues

1. **Authentication Failed**
   - Verify username and password are correct
   - Check if the user has proper permissions
   - Ensure the user account is not locked

2. **Connection Timeout**
   - Verify the host URL is correct
   - Check network connectivity
   - Ensure firewall allows Snowflake connections

3. **Database/Schema Not Found**
   - Verify the database name in connection extra
   - Check if the user has access to the specified database
   - Ensure the database exists

4. **Warehouse Not Found**
   - Verify the warehouse name in connection extra
   - Check if the user has access to the warehouse
   - Ensure the warehouse exists and is not suspended

### Customization

To use a different connection ID, modify the DAG:

```python
# Change this line in the test_snowflake_connection task
snowflake_hook = SnowflakeHook(snowflake_conn_id="your_connection_id")
conn = snowflake_hook.get_connection("your_connection_id")
```

## Next Steps

Once the connection test passes, you can:

1. **Create Data Pipelines**: Use the verified connection for ETL/ELT processes
2. **Set Up Monitoring**: Create DAGs to monitor Snowflake warehouse usage
3. **Data Quality Checks**: Implement data validation workflows
4. **Scheduled Jobs**: Set up regular data processing schedules

## Support

For issues with this DAG or Snowflake connectivity:
- Check the Airflow logs for detailed error messages
- Verify your Snowflake connection configuration
- Ensure all required packages are installed
- Contact your Astro support team for assistance
