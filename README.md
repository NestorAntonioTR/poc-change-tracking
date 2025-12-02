# SQL Server to BigQuery Data Replication Tool

This tool replicates data from SQL Server tables with change tracking enabled to Google BigQuery. It supports both initial full loads and incremental updates based on SQL Server's change tracking feature.

## Features

- **Initial Full Load**: Automatically performs a full data load for new tables
- **Incremental Updates**: Uses SQL Server's change tracking to efficiently replicate only changed data
- **Change Detection**: Identifies inserts, updates, and deletes in source tables
- **Batched Processing**: Processes large tables in configurable batches to manage memory usage
- **State Management**: Tracks replication state to resume from the last successful sync
- **Robust Error Handling**: Comprehensive logging and error management
- **Type Mapping**: Automatically maps SQL Server data types to appropriate BigQuery types

## Prerequisites

- SQL Server with Change Tracking enabled on the source database and tables
- Google Cloud project with BigQuery API enabled
- Service account with BigQuery Data Editor permissions
- Python 3.6+
- ODBC Driver for SQL Server

## Installation

1. Clone this repository or download the files to your local machine.

2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

3. Create a `.env` file based on the provided `.env.example`:

```bash
cp .env.example .env
```

4. Edit the `.env` file with your connection details and configuration.

## Configuration

Edit the `.env` file with your specific configuration:

### SQL Server Connection

```
SQL_SERVER=your_server_name
SQL_DATABASE=your_database_name
SQL_USERNAME=your_username
SQL_PASSWORD=your_password
SQL_DRIVER={ODBC Driver 17 for SQL Server}
```

### BigQuery Connection

```
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/credentials.json
BQ_PROJECT_ID=your_gcp_project_id
BQ_DATASET=your_bigquery_dataset
```

### Replication Settings

```
TABLES_TO_REPLICATE=table1,table2,table3
BATCH_SIZE=1000
LOG_LEVEL=INFO
```

## Enabling Change Tracking in SQL Server

Before using this tool, you need to enable change tracking on your SQL Server database and tables:

1. Enable change tracking on the database:

```sql
ALTER DATABASE YourDatabase
SET CHANGE_TRACKING = ON
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)
```

2. Enable change tracking on each table you want to replicate:

```sql
ALTER TABLE YourTable
ENABLE CHANGE_TRACKING
WITH (TRACK_COLUMNS_UPDATED = ON)
```

## Usage

Run the script to start the replication process:

```bash
python sql_to_bq_replicator.py
```

### Scheduling Regular Syncs

For regular synchronization, you can set up a cron job or scheduled task:

#### Linux/Mac (Cron)

```bash
# Run every hour
0 * * * * cd /path/to/replicator && python sql_to_bq_replicator.py
```

#### Windows (Task Scheduler)

Create a scheduled task that runs the script at your desired interval.

## How It Works

1. **Initial Run**: On the first run for each table, the script performs a full data load to BigQuery.

2. **State Tracking**: The script saves the change tracking version after each successful sync in `replication_state.json`.

3. **Subsequent Runs**: On subsequent runs, the script:
   - Retrieves the last sync version from the state file
   - Queries SQL Server for changes since that version
   - Applies those changes (inserts, updates, deletes) to BigQuery
   - Updates the state file with the new version

4. **Change Processing**:
   - Inserts and updates are loaded directly to BigQuery
   - Deletes are processed by removing the corresponding rows from BigQuery

## Logging

The script logs detailed information to both the console and a `replication.log` file. You can adjust the log level in the `.env` file.

## Troubleshooting

### Common Issues

1. **Connection Errors**:
   - Verify your SQL Server and BigQuery connection details
   - Ensure your SQL Server allows remote connections
   - Check that your Google Cloud service account has the necessary permissions

2. **Change Tracking Issues**:
   - Verify change tracking is enabled on both the database and tables
   - Check the retention period is sufficient for your sync frequency

3. **Data Type Compatibility**:
   - If you encounter data type errors, you may need to modify the `_convert_schema` method in the script

### Debugging

For more detailed logging, set `LOG_LEVEL=DEBUG` in your `.env` file.

## Limitations

- Tables must have a primary key for change tracking to work properly
- Very large tables may require adjustments to the `BATCH_SIZE` setting
- SQL Server-specific data types may not have perfect BigQuery equivalents

## License

This project is licensed under the MIT License - see the LICENSE file for details.
