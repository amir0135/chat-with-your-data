# Trackman Data Integration

This solution accelerator now supports querying Trackman operational data through a custom data tool integration.

## Overview

The Trackman integration provides access to operational data including:
- **Error tracking**: Monitor facility errors, severity levels, and error patterns
- **Connectivity status**: Track unit connectivity and disconnect reasons
- **Facility metrics**: View comprehensive facility summaries and metadata
- **Data quality**: Monitor data quality scores, missing records, and latency

## Architecture

The integration uses a data source abstraction with two implementations:

1. **ExcelDataSource** (default): Reads from a local Excel file for development/demo
2. **RedshiftDataSource**: Connects to Amazon Redshift for production data

## Data Source Configuration

### Excel Mode (Default)

The Excel data source scans a directory for all Excel files and automatically merges data from matching sheets.

**Default directory**: `data/testtrack/`

**How it works**:
1. System scans for all `.xlsx` and `.xls` files in the directory
2. For each expected sheet (errors, connectivity, facility_metadata, data_quality), it loads data from all files
3. Data from matching sheets across files is automatically merged
4. Duplicate rows are removed

**Configuration**:
```bash
# Optional - specify a different directory
TRACKMAN_DATA_DIR=/path/to/your/excel/files
```

**Example**:
```
data/testtrack/
├── facility_a_errors.xlsx      (contains "errors" sheet)
├── facility_b_errors.xlsx      (contains "errors" sheet)
├── all_facilities_meta.xlsx    (contains "facility_metadata" sheet)
└── connectivity_data.xlsx      (contains "connectivity" sheet)
```

All error data from facility_a and facility_b will be merged, along with metadata and connectivity from the other files.

### Redshift Mode

To enable Redshift integration, set the following environment variables:

```bash
USE_REDSHIFT=true
REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DB=your_database
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password
REDSHIFT_SCHEMA=public  # optional, defaults to public
```

## Excel File Format

The Excel file must contain these sheets with the specified columns:

### errors
- timestamp
- facility_id
- unit_id
- unit_model
- error_code
- severity
- error_message

### connectivity
- timestamp
- facility_id
- unit_id
- connectivity_status
- disconnect_reason

### facility_metadata
- facility_id
- location
- opening_hours
- subscription_status
- units_deployed
- usage_hours_30d
- strokes_tracked
- tournaments_hosted

### data_quality
- timestamp
- facility_id
- data_quality_score
- missing_records
- latency_ms

## Security Features

### Redshift Security

The Redshift integration includes multiple security layers:

1. **Parameterized Queries**: All queries use parameterized SQL to prevent SQL injection
2. **Table Allowlist**: Only predefined tables can be queried (configured in `redshift_config.py`)
3. **Column Allowlist**: Only approved columns can be accessed per table
4. **No Dynamic SQL**: User input never directly constructs SQL queries

### Configuration File

The allowlist configuration is in:
```
code/backend/batch/utilities/helpers/trackman/redshift_config.py
```

To add new tables or columns, edit the `ALLOWED_TABLES` dictionary in this file.

## Usage Examples

Users can ask natural language questions like:

**Error Queries:**
- "Show me error summary for the last 7 days"
- "What are the top error messages at FAC001?"
- "How many critical errors occurred this week?"

**Connectivity Queries:**
- "What's the connectivity status for all facilities?"
- "Why are units disconnecting at FAC002?"
- "Show disconnect reasons for the past month"

**Facility Queries:**
- "Give me a summary of facility FAC001"
- "What's the subscription status of FAC003?"
- "How many tournaments has FAC002 hosted?"

**Data Quality Queries:**
- "What's the data quality for all facilities?"
- "Show data quality issues for FAC001"
- "Which facilities have low data quality scores?"

## Query Intents

The tool supports these query intents:

- `errors_summary`: Get error counts and severity breakdown by facility
- `top_error_messages`: Get most frequent error messages
- `connectivity_summary`: Get connectivity statistics
- `disconnect_reasons`: Get breakdown of disconnect reasons
- `facility_summary`: Get comprehensive facility information
- `data_quality_summary`: Get data quality metrics

## Response Format

All queries return data in a consistent JSON format:

```json
{
  "columns": ["col1", "col2", ...],
  "rows": [[val1, val2, ...], ...],
  "metadata": {
    "source": "excel" or "redshift",
    "range_days": 30,
    "rowCount": 10,
    "facility_id": "FAC001"  // if filtered
  }
}
```

The chat assistant formats this as markdown tables with summaries.

## Testing

Run the test suite:

```bash
poetry run pytest code/tests/test_trackman_data_sources.py -v
```

Tests cover:
- Excel data source functionality
- Redshift parameterized query verification
- Table/column allowlist enforcement
- Data source factory selection logic

## Development Workflow

1. **Start with Excel**: Use the Excel fallback for development and testing
2. **Test locally**: Verify queries work with sample data
3. **Switch to Redshift**: Set environment variables to connect to actual database
4. **Validate**: Ensure queries return expected results
5. **Monitor**: Check logs for data source initialization and query execution

## Troubleshooting

### Excel file not found
```
Error: No such file or directory: 'data/trackman_test_data.xlsx'
```
**Solution**: Place your Excel file in the data directory or set `TRACKMAN_EXCEL_PATH`

### Redshift connection failed
```
Error: Missing required Redshift environment variables
```
**Solution**: Verify all `REDSHIFT_*` variables are set and `USE_REDSHIFT=true`

### Table not in allowlist
```
Error: Table 'xyz' is not in the allowlist
```
**Solution**: Add the table to `ALLOWED_TABLES` in `redshift_config.py`

### Empty results
- Check date range (default is 30 days)
- Verify facility_id is correct
- Ensure data exists in the specified time range

## Logs

The system logs data source initialization and query execution:

```
INFO: Initializing Excel data source (USE_REDSHIFT not set to true)
INFO: Excel data source active
INFO: Loaded sheet 'errors' with 150 rows
```

Check logs to verify which data source is active and troubleshoot issues.

## Best Practices

1. **Always use parameterized queries** - Never concatenate user input into SQL
2. **Maintain the allowlist** - Keep approved tables/columns up to date
3. **Test with Excel first** - Validate query logic before connecting to Redshift
4. **Monitor query performance** - Log slow queries and optimize as needed
5. **Keep data fresh** - Ensure Excel file is updated for demos/testing

## Future Enhancements

Potential additions:
- Caching layer for frequently accessed data
- Additional query types (trends, comparisons, anomaly detection)
- Real-time data streaming for connectivity status
- Dashboard views for facility health
- Alert thresholds and notifications
