"""
Generate sample Trackman test data in Excel format.
This script creates realistic test data for development and demo purposes.
"""

import pandas as pd
from datetime import datetime, timedelta
import random
import sys
from pathlib import Path

def generate_test_data(output_path: str = "data/testtrack/trackman_test_data.xlsx"):
    """Generate sample Trackman data with realistic values."""

    # Generate timestamps for the past 30 days
    base_time = datetime.now()
    timestamps = [
        base_time - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
        for _ in range(100)
    ]

    # Errors sheet
    errors_data = {
        'timestamp': [timestamps[i] for i in range(50)],
        'facility_id': [f'FAC{str(i%3 + 1).zfill(3)}' for i in range(50)],
        'unit_id': [f'UNIT{str(i%10 + 1).zfill(3)}' for i in range(50)],
        'unit_model': [random.choice(['TrackMan 4', 'TrackMan 4+', 'TrackMan Range']) for _ in range(50)],
        'error_code': [random.choice(['E001', 'E002', 'E003', 'E004', 'E005']) for _ in range(50)],
        'severity': [random.choice(['INFO', 'WARNING', 'ERROR', 'CRITICAL']) for _ in range(50)],
        'error_message': [random.choice([
            'Sensor calibration failed',
            'Network connection timeout',
            'Data synchronization error',
            'Battery low warning',
            'Temperature threshold exceeded',
            'Camera alignment issue',
            'Radar initialization failed'
        ]) for _ in range(50)]
    }

    # Connectivity sheet
    connectivity_data = {
        'timestamp': [timestamps[i] for i in range(50, 100)],
        'facility_id': [f'FAC{str(i%3 + 1).zfill(3)}' for i in range(50)],
        'unit_id': [f'UNIT{str(i%10 + 1).zfill(3)}' for i in range(50)],
        'connectivity_status': [random.choice(['ONLINE', 'OFFLINE']) for _ in range(50)],
        'disconnect_reason': [random.choice([
            'Network Timeout',
            'Power Loss',
            'Manual Disconnect',
            'Firmware Update',
            'Connection Refused',
            None
        ]) for _ in range(50)]
    }

    # Facility metadata sheet
    facility_metadata_data = {
        'facility_id': ['FAC001', 'FAC002', 'FAC003'],
        'location': ['New York, NY', 'Los Angeles, CA', 'Chicago, IL'],
        'opening_hours': ['6 AM - 11 PM', '5 AM - 10 PM', '7 AM - 9 PM'],
        'subscription_status': ['ACTIVE', 'ACTIVE', 'TRIAL'],
        'units_deployed': [15, 22, 8],
        'usage_hours_30d': [3200, 4800, 1200],
        'strokes_tracked': [125000, 185000, 42000],
        'tournaments_hosted': [3, 5, 1]
    }

    # Data quality sheet
    data_quality_data = {
        'timestamp': [base_time - timedelta(days=i) for i in range(30)],
        'facility_id': [f'FAC{str((i%3) + 1).zfill(3)}' for i in range(30)],
        'data_quality_score': [round(random.uniform(0.85, 0.99), 4) for _ in range(30)],
        'missing_records': [random.randint(0, 50) for _ in range(30)],
        'latency_ms': [random.randint(50, 500) for _ in range(30)]
    }

    # Create DataFrames
    errors_df = pd.DataFrame(errors_data)
    connectivity_df = pd.DataFrame(connectivity_data)
    facility_metadata_df = pd.DataFrame(facility_metadata_data)
    data_quality_df = pd.DataFrame(data_quality_data)

    # Ensure output directory exists
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Write to Excel with multiple sheets
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        errors_df.to_excel(writer, sheet_name='errors', index=False)
        connectivity_df.to_excel(writer, sheet_name='connectivity', index=False)
        facility_metadata_df.to_excel(writer, sheet_name='facility_metadata', index=False)
        data_quality_df.to_excel(writer, sheet_name='data_quality', index=False)

    print(f'✓ Successfully created {output_path}')
    print(f'  - errors: {len(errors_df)} rows')
    print(f'  - connectivity: {len(connectivity_df)} rows')
    print(f'  - facility_metadata: {len(facility_metadata_df)} rows')
    print(f'  - data_quality: {len(data_quality_df)} rows')

    return output_path

if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "data/trackman_test_data.xlsx"
    generate_test_data(output)
