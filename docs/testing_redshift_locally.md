# Testing Redshift Integration Locally

Since Amazon Redshift is based on PostgreSQL, you can test the integration locally using PostgreSQL.

## Option 1: Docker PostgreSQL (Easiest)

### 1. Start PostgreSQL with Docker

```bash
docker run --name trackman-postgres \
  -e POSTGRES_PASSWORD=testpassword \
  -e POSTGRES_USER=testuser \
  -e POSTGRES_DB=trackman_test \
  -p 5432:5432 \
  -d postgres:14
```

### 2. Create Test Tables and Data

```bash
# Connect to the database
docker exec -it trackman-postgres psql -U testuser -d trackman_test
```

Then run this SQL:

```sql
-- Create errors table
CREATE TABLE errors (
    timestamp TIMESTAMP,
    facility_id VARCHAR(50),
    unit_id VARCHAR(50),
    unit_model VARCHAR(50),
    error_code VARCHAR(50),
    severity VARCHAR(20),
    error_message TEXT
);

-- Create connectivity table
CREATE TABLE connectivity (
    timestamp TIMESTAMP,
    facility_id VARCHAR(50),
    unit_id VARCHAR(50),
    connectivity_status VARCHAR(20),
    disconnect_reason VARCHAR(200)
);

-- Create facility_metadata table
CREATE TABLE facility_metadata (
    facility_id VARCHAR(50) PRIMARY KEY,
    location VARCHAR(200),
    opening_hours VARCHAR(100),
    subscription_status VARCHAR(20),
    units_deployed INTEGER,
    usage_hours_30d FLOAT,
    strokes_tracked INTEGER,
    tournaments_hosted INTEGER
);

-- Create data_quality table
CREATE TABLE data_quality (
    timestamp TIMESTAMP,
    facility_id VARCHAR(50),
    data_quality_score FLOAT,
    missing_records INTEGER,
    latency_ms FLOAT
);

-- Insert sample data
INSERT INTO errors (timestamp, facility_id, unit_id, unit_model, error_code, severity, error_message)
VALUES
    (NOW() - INTERVAL '1 day', 'FAC001', 'UNIT001', 'TM4', 'ERR_CAL', 'HIGH', 'Calibration failure detected'),
    (NOW() - INTERVAL '2 days', 'FAC001', 'UNIT002', 'TM5', 'ERR_CONN', 'CRITICAL', 'Network connection lost'),
    (NOW() - INTERVAL '3 days', 'FAC002', 'UNIT003', 'TM4', 'ERR_SENSOR', 'MEDIUM', 'Sensor reading out of range');

INSERT INTO connectivity (timestamp, facility_id, unit_id, connectivity_status, disconnect_reason)
VALUES
    (NOW() - INTERVAL '1 hour', 'FAC001', 'UNIT001', 'ONLINE', NULL),
    (NOW() - INTERVAL '2 hours', 'FAC001', 'UNIT002', 'OFFLINE', 'Network timeout'),
    (NOW() - INTERVAL '1 day', 'FAC002', 'UNIT003', 'ONLINE', NULL);

INSERT INTO facility_metadata (facility_id, location, opening_hours, subscription_status, units_deployed, usage_hours_30d, strokes_tracked, tournaments_hosted)
VALUES
    ('FAC001', 'New York, NY', '6AM-10PM', 'ACTIVE', 5, 720.5, 125000, 12),
    ('FAC002', 'Los Angeles, CA', '7AM-9PM', 'ACTIVE', 3, 480.0, 85000, 8);

INSERT INTO data_quality (timestamp, facility_id, data_quality_score, missing_records, latency_ms)
VALUES
    (NOW() - INTERVAL '1 hour', 'FAC001', 95.5, 12, 45.2),
    (NOW() - INTERVAL '2 hours', 'FAC001', 94.8, 15, 52.1),
    (NOW() - INTERVAL '1 day', 'FAC002', 98.2, 3, 32.5);
```

### 3. Configure Environment Variables

Update your `.env` file:

```bash
USE_REDSHIFT=true
REDSHIFT_HOST=localhost
REDSHIFT_PORT=5432
REDSHIFT_DB=trackman_test
REDSHIFT_USER=testuser
REDSHIFT_PASSWORD=testpassword
REDSHIFT_SCHEMA=public
```

### 4. Test the Integration

Restart your admin app and chat interface. You can now test queries like:
- "Show me errors from the last 7 days"
- "What's the connectivity status for facility FAC001?"
- "Give me a facility summary for FAC001"

### 5. Clean Up

When done testing:
```bash
docker stop trackman-postgres
docker rm trackman-postgres
```

## Option 2: Install PostgreSQL Locally

### macOS (using Homebrew)
```bash
brew install postgresql@14
brew services start postgresql@14

# Create database
createdb trackman_test

# Connect and run the SQL above
psql trackman_test
```

### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib

# Create user and database
sudo -u postgres createuser testuser
sudo -u postgres createdb trackman_test
sudo -u postgres psql -c "ALTER USER testuser WITH PASSWORD 'testpassword';"

# Connect and run the SQL above
psql -U testuser -d trackman_test
```

## Option 3: Run Unit Tests (No Database Required)

We have unit tests that mock the Redshift connection:

```bash
# Run Redshift integration tests
poetry run pytest tests/test_trackman_redshift.py -v

# Run all Trackman tests
poetry run pytest tests/ -k trackman -v
```

These tests verify:
- SQL parameterization
- Allowlist enforcement
- Query correctness
- Error handling

## Option 4: Use Excel Mode (Skip Redshift Testing)

If you just want to test the feature end-to-end without Redshift:

1. Keep `USE_REDSHIFT=false` (or comment it out)
2. Place Excel files in `data/testtrack/`
3. Test the same queries - they'll work against Excel data

The integration layer is identical for both data sources, so if it works with Excel, it will work with Redshift.

## Verifying the Integration Works

### Check Logs

When the app starts, you should see:
```
INFO: Trackman data source initialized: RedshiftDataSource
INFO: Connected to Redshift at localhost:5432/trackman_test
```

Or with Excel:
```
INFO: Trackman data source initialized: ExcelDataSource
INFO: Found 2 Excel file(s) in data/testtrack
```

### Test Queries in Chat

Try these questions in the chat interface:
1. "Show me all critical errors from the last week"
2. "What facilities had connectivity issues yesterday?"
3. "Give me a summary for facility FAC001"
4. "What's the data quality score trend?"

The assistant should respond with tables and summaries.

### Manual Tool Testing

You can also test the tool directly in Python:

```python
from code.backend.batch.utilities.tools.trackman_query_tool import TrackmanQueryTool

tool = TrackmanQueryTool()

# Test errors query
result = tool.query_trackman_data(
    intent="errors_summary",
    facility_id="FAC001",
    range_days=7
)
print(result)
```

## Troubleshooting

### Connection refused
- Make sure PostgreSQL is running: `docker ps` or `brew services list`
- Check the port is correct (5432 for PostgreSQL, 5439 for Redshift)

### Authentication failed
- Verify username/password in `.env` match database settings
- Try connecting manually: `psql -h localhost -U testuser -d trackman_test`

### Table not found
- Ensure you ran all the CREATE TABLE statements
- Check schema: tables should be in `public` schema
- Verify allowlist in `redshift_config.py` includes your tables

### No data returned
- Verify INSERT statements ran successfully
- Check date ranges - sample data uses recent timestamps
- Try a query without date filter first
