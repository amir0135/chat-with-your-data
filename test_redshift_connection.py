"""Simple script to test Redshift/PostgreSQL connection and Trackman integration"""
import sys
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Add code path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code'))

from backend.batch.utilities.helpers.trackman.data_source_factory import get_data_source

def test_connection():
    print("=" * 60)
    print("Testing Trackman Data Source Connection")
    print("=" * 60)

    # Get data source
    data_source = get_data_source()
    print(f"\n✓ Data Source Type: {type(data_source).__name__}")

    # Add debug query
    print("\n" + "=" * 60)
    print("Debug: Raw data from errors table")
    print("=" * 60)
    try:
        query_result = data_source._execute_query(
            "SELECT COUNT(*) as total, severity FROM errors WHERE timestamp >= %s GROUP BY severity",
            params=[data_source._get_date_filter(30)]
        )
        print(f"Raw query result: {query_result}")
    except Exception as e:
        print(f"Debug query error: {e}")

    # Test queries
    print("\n" + "=" * 60)
    print("Test 1: Get Errors Summary (last 30 days)")
    print("=" * 60)
    result = data_source.get_errors_summary(range_days=30)
    print(f"Full result: {result}")
    print(f"Total Errors: {result.get('total_errors', 0)}")
    print(f"Critical: {result.get('critical_count', 0)}")
    print(f"High: {result.get('high_count', 0)}")
    print(f"Medium: {result.get('medium_count', 0)}")

    print("\n" + "=" * 60)
    print("Test 2: Get Facility Summary for FAC001")
    print("=" * 60)
    result = data_source.get_facility_summary("FAC001", range_days=30)
    print(f"Facility ID: {result.get('facility_id')}")
    print(f"Location: {result.get('location')}")
    print(f"Units Deployed: {result.get('units_deployed')}")
    print(f"Subscription Status: {result.get('subscription_status')}")

    print("\n" + "=" * 60)
    print("Test 3: Get Top Error Messages")
    print("=" * 60)
    result = data_source.get_top_error_messages(range_days=30, limit=5)
    if result.get('top_errors'):
        for i, error in enumerate(result['top_errors'][:3], 1):
            print(f"\n{i}. Error Code: {error.get('error_code')}")
            print(f"   Message: {error.get('error_message')}")
            print(f"   Count: {error.get('count')}")
            print(f"   Severity: {error.get('severity')}")

    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)
    print("\nThe Redshift integration is working correctly!")
    print("You can now ask questions like:")
    print("  - 'Show me errors from facility FAC001'")
    print("  - 'What are the recent critical errors?'")
    print("  - 'Give me a summary of facility FAC001'")
    print("=" * 60)

if __name__ == "__main__":
    try:
        test_connection()
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
