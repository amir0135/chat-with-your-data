#!/bin/bash

# Setup script for testing Redshift integration locally using PostgreSQL

set -e

echo "=========================================="
echo "Trackman Redshift Local Test Setup"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Stop and remove existing container if it exists
if docker ps -a | grep -q trackman-postgres; then
    echo "📦 Removing existing trackman-postgres container..."
    docker stop trackman-postgres 2>/dev/null || true
    docker rm trackman-postgres 2>/dev/null || true
fi

# Start PostgreSQL container
echo "🚀 Starting PostgreSQL container..."
docker run --name trackman-postgres \
  -e POSTGRES_PASSWORD=testpassword \
  -e POSTGRES_USER=testuser \
  -e POSTGRES_DB=trackman_test \
  -p 5432:5432 \
  -d postgres:14

echo "⏳ Waiting for PostgreSQL to be ready..."
sleep 5

# Wait for PostgreSQL to accept connections
until docker exec trackman-postgres pg_isready -U testuser > /dev/null 2>&1; do
  echo "   Still waiting..."
  sleep 2
done

echo "✅ PostgreSQL is ready"
echo ""

# Create tables and insert test data
echo "📊 Creating tables and inserting test data..."

docker exec -i trackman-postgres psql -U testuser -d trackman_test << 'EOF'

-- Create errors table
CREATE TABLE IF NOT EXISTS errors (
    timestamp TIMESTAMP,
    facility_id VARCHAR(50),
    unit_id VARCHAR(50),
    unit_model VARCHAR(50),
    error_code VARCHAR(50),
    severity VARCHAR(20),
    error_message TEXT
);

-- Create connectivity table
CREATE TABLE IF NOT EXISTS connectivity (
    timestamp TIMESTAMP,
    facility_id VARCHAR(50),
    unit_id VARCHAR(50),
    connectivity_status VARCHAR(20),
    disconnect_reason VARCHAR(200)
);

-- Create facility_metadata table
CREATE TABLE IF NOT EXISTS facility_metadata (
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
CREATE TABLE IF NOT EXISTS data_quality (
    timestamp TIMESTAMP,
    facility_id VARCHAR(50),
    data_quality_score FLOAT,
    missing_records INTEGER,
    latency_ms FLOAT
);

-- Clear existing data
TRUNCATE errors, connectivity, facility_metadata, data_quality;

-- Insert sample errors
INSERT INTO errors (timestamp, facility_id, unit_id, unit_model, error_code, severity, error_message)
VALUES
    (NOW() - INTERVAL '1 day', 'FAC001', 'UNIT001', 'TM4', 'ERR_CAL', 'HIGH', 'Calibration failure detected'),
    (NOW() - INTERVAL '2 days', 'FAC001', 'UNIT002', 'TM5', 'ERR_CONN', 'CRITICAL', 'Network connection lost'),
    (NOW() - INTERVAL '3 days', 'FAC002', 'UNIT003', 'TM4', 'ERR_SENSOR', 'MEDIUM', 'Sensor reading out of range'),
    (NOW() - INTERVAL '5 days', 'FAC001', 'UNIT001', 'TM4', 'ERR_POWER', 'LOW', 'Power fluctuation detected'),
    (NOW() - INTERVAL '6 days', 'FAC003', 'UNIT004', 'TM5', 'ERR_CAL', 'CRITICAL', 'Camera calibration failed'),
    (NOW() - INTERVAL '1 hour', 'FAC002', 'UNIT003', 'TM4', 'ERR_TEMP', 'MEDIUM', 'Operating temperature high');

-- Insert connectivity data
INSERT INTO connectivity (timestamp, facility_id, unit_id, connectivity_status, disconnect_reason)
VALUES
    (NOW() - INTERVAL '1 hour', 'FAC001', 'UNIT001', 'ONLINE', NULL),
    (NOW() - INTERVAL '2 hours', 'FAC001', 'UNIT002', 'OFFLINE', 'Network timeout'),
    (NOW() - INTERVAL '3 hours', 'FAC002', 'UNIT003', 'ONLINE', NULL),
    (NOW() - INTERVAL '1 day', 'FAC003', 'UNIT004', 'OFFLINE', 'Power outage'),
    (NOW() - INTERVAL '2 days', 'FAC001', 'UNIT001', 'OFFLINE', 'Firmware update'),
    (NOW() - INTERVAL '30 minutes', 'FAC003', 'UNIT004', 'ONLINE', NULL);

-- Insert facility metadata
INSERT INTO facility_metadata (facility_id, location, opening_hours, subscription_status, units_deployed, usage_hours_30d, strokes_tracked, tournaments_hosted)
VALUES
    ('FAC001', 'New York, NY', '6AM-10PM', 'ACTIVE', 5, 720.5, 125000, 12),
    ('FAC002', 'Los Angeles, CA', '7AM-9PM', 'ACTIVE', 3, 480.0, 85000, 8),
    ('FAC003', 'Chicago, IL', '7AM-11PM', 'TRIAL', 2, 240.5, 45000, 3);

-- Insert data quality metrics
INSERT INTO data_quality (timestamp, facility_id, data_quality_score, missing_records, latency_ms)
VALUES
    (NOW() - INTERVAL '1 hour', 'FAC001', 95.5, 12, 45.2),
    (NOW() - INTERVAL '2 hours', 'FAC001', 94.8, 15, 52.1),
    (NOW() - INTERVAL '3 hours', 'FAC002', 98.2, 3, 32.5),
    (NOW() - INTERVAL '1 day', 'FAC002', 97.5, 8, 38.0),
    (NOW() - INTERVAL '1 day', 'FAC003', 92.1, 25, 68.5),
    (NOW() - INTERVAL '2 days', 'FAC001', 96.2, 10, 42.0);

EOF

echo "✅ Tables created and data inserted"
echo ""

# Verify data
echo "📋 Data Summary:"
docker exec trackman-postgres psql -U testuser -d trackman_test -c "
SELECT
    (SELECT COUNT(*) FROM errors) as error_count,
    (SELECT COUNT(*) FROM connectivity) as connectivity_count,
    (SELECT COUNT(*) FROM facility_metadata) as facility_count,
    (SELECT COUNT(*) FROM data_quality) as quality_count;
"

echo ""
echo "=========================================="
echo "✅ Setup Complete!"
echo "=========================================="
echo ""
echo "📝 Next steps:"
echo ""
echo "1. Update your .env file with:"
echo "   USE_REDSHIFT=true"
echo "   REDSHIFT_HOST=localhost"
echo "   REDSHIFT_PORT=5432"
echo "   REDSHIFT_DB=trackman_test"
echo "   REDSHIFT_USER=testuser"
echo "   REDSHIFT_PASSWORD=testpassword"
echo "   REDSHIFT_SCHEMA=public"
echo ""
echo "2. Restart your application"
echo ""
echo "3. Test with queries like:"
echo "   - 'Show me critical errors from the last 7 days'"
echo "   - 'What is the connectivity status for facility FAC001?'"
echo "   - 'Give me a summary for facility FAC001'"
echo ""
echo "=========================================="
echo ""
echo "🔧 Useful commands:"
echo "   Connect to database:"
echo "     docker exec -it trackman-postgres psql -U testuser -d trackman_test"
echo ""
echo "   View logs:"
echo "     docker logs trackman-postgres"
echo ""
echo "   Stop container:"
echo "     docker stop trackman-postgres"
echo ""
echo "   Remove container:"
echo "     docker rm trackman-postgres"
echo ""
