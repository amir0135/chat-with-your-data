#!/usr/bin/env python3
"""
Populate the local PostgreSQL test database with sample Trackman data.
This script creates the same test data that would exist in Redshift.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Database connection parameters (matching setup_trackman_test_db.sh)
DB_CONFIG = {
    "host": os.getenv("REDSHIFT_HOST", "localhost"),
    "port": int(os.getenv("REDSHIFT_PORT", "5432")),
    "database": os.getenv("REDSHIFT_DB", "trackman_test"),
    "user": os.getenv("REDSHIFT_USER", "trackman_user"),
    "password": os.getenv("REDSHIFT_PASSWORD", "trackman_password"),
}


def generate_sample_data():
    """Generate sample data for all tables."""

    # Facilities
    facilities = [
        ("FAC001", "New York Golf Center", "Mon-Sun 6am-10pm", "ACTIVE", 5, 245.5, 125000, 8),
        ("FAC002", "Los Angeles Training", "Mon-Fri 7am-9pm", "ACTIVE", 3, 180.0, 89000, 5),
        ("FAC003", "Chicago Sports Academy", "Mon-Sun 8am-8pm", "TRIAL", 4, 120.5, 45000, 2),
        ("FAC004", "Houston Golf Club", "Tue-Sun 9am-7pm", "ACTIVE", 6, 310.0, 178000, 12),
        ("FAC005", "Miami Beach Golf", "Mon-Sun 7am-11pm", "EXPIRED", 2, 45.0, 12000, 0),
    ]

    # Generate errors for last 30 days
    errors = []
    base_date = datetime.now()
    facilities_ids = [f[0] for f in facilities]

    error_templates = [
        ("E001", "LOW", "Sensor calibration drift detected"),
        ("E002", "MEDIUM", "Camera sync timeout"),
        ("E003", "HIGH", "Ball tracking lost"),
        ("E004", "CRITICAL", "System memory overflow"),
        ("E005", "LOW", "Network latency spike"),
        ("E006", "MEDIUM", "Storage capacity warning"),
        ("E007", "HIGH", "Hardware temperature exceeded"),
        ("E008", "CRITICAL", "Database connection failed"),
    ]

    unit_models = ["TrackMan 4", "TrackMan iO", "TrackMan Range"]

    for day in range(30):
        date = base_date - timedelta(days=day)
        num_errors = 5 + (day % 10)  # Varying number of errors per day

        for i in range(num_errors):
            facility_id = facilities_ids[i % len(facilities_ids)]
            unit_id = f"{facility_id}-U{(i % 3) + 1}"
            unit_model = unit_models[i % len(unit_models)]
            error_code, severity, message = error_templates[i % len(error_templates)]

            timestamp = date - timedelta(hours=i % 24, minutes=(i * 13) % 60)
            errors.append((timestamp, facility_id, unit_id, unit_model, error_code, severity, message))

    # Generate connectivity data
    connectivity = []
    for day in range(30):
        date = base_date - timedelta(days=day)

        for fac_id in facilities_ids:
            for unit_num in range(1, 4):
                unit_id = f"{fac_id}-U{unit_num}"

                # Most units online, occasional offline
                status = "ONLINE" if (day + unit_num) % 7 != 0 else "OFFLINE"
                reason = None if status == "ONLINE" else ["Network timeout", "Power loss", "Manual disconnect"][unit_num % 3]

                timestamp = date - timedelta(hours=8 + unit_num * 4)
                connectivity.append((timestamp, fac_id, unit_id, status, reason))

    # Generate data quality metrics
    data_quality = []
    for day in range(30):
        date = base_date - timedelta(days=day)

        for fac_id in facilities_ids:
            # Quality score varies by facility and day
            base_score = 85.0 + (hash(fac_id) % 10)
            daily_variance = (day % 15) - 7
            score = min(100.0, max(60.0, base_score + daily_variance))

            missing = int((100 - score) * 0.5)
            latency = 50.0 + (100 - score) * 2.0

            timestamp = date - timedelta(hours=12)
            data_quality.append((timestamp, fac_id, score, missing, latency))

    return facilities, errors, connectivity, data_quality


def populate_database():
    """Populate the database with sample data."""

    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        print("Generating sample data...")
        facilities, errors, connectivity, data_quality = generate_sample_data()

        # Insert facilities
        print(f"Inserting {len(facilities)} facilities...")
        execute_values(
            cursor,
            """
            INSERT INTO facility_metadata
            (facility_id, location, opening_hours, subscription_status,
             units_deployed, usage_hours_30d, strokes_tracked, tournaments_hosted)
            VALUES %s
            ON CONFLICT (facility_id) DO UPDATE SET
                location = EXCLUDED.location,
                opening_hours = EXCLUDED.opening_hours,
                subscription_status = EXCLUDED.subscription_status,
                units_deployed = EXCLUDED.units_deployed,
                usage_hours_30d = EXCLUDED.usage_hours_30d,
                strokes_tracked = EXCLUDED.strokes_tracked,
                tournaments_hosted = EXCLUDED.tournaments_hosted
            """,
            facilities
        )

        # Insert errors
        print(f"Inserting {len(errors)} error records...")
        execute_values(
            cursor,
            """
            INSERT INTO errors
            (timestamp, facility_id, unit_id, unit_model, error_code, severity, error_message)
            VALUES %s
            """,
            errors
        )

        # Insert connectivity
        print(f"Inserting {len(connectivity)} connectivity records...")
        execute_values(
            cursor,
            """
            INSERT INTO connectivity
            (timestamp, facility_id, unit_id, connectivity_status, disconnect_reason)
            VALUES %s
            """,
            connectivity
        )

        # Insert data quality
        print(f"Inserting {len(data_quality)} data quality records...")
        execute_values(
            cursor,
            """
            INSERT INTO data_quality
            (timestamp, facility_id, data_quality_score, missing_records, latency_ms)
            VALUES %s
            """,
            data_quality
        )

        conn.commit()
        print("\n✅ Database populated successfully!")

        # Print summary
        cursor.execute("SELECT COUNT(*) FROM facility_metadata")
        print(f"   - Facilities: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM errors")
        print(f"   - Error records: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM connectivity")
        print(f"   - Connectivity records: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM data_quality")
        print(f"   - Data quality records: {cursor.fetchone()[0]}")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error populating database: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("Trackman Test Database Population Script")
    print("=" * 60)

    # Check if psycopg2 is available
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        print("\n❌ Error: psycopg2 is not installed")
        print("Install it with: pip install psycopg2-binary")
        sys.exit(1)

    populate_database()
