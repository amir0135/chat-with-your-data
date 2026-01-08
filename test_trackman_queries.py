"""Interactive test script for Trackman/Redshift integration"""
import sys
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Add code path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code'))

from backend.batch.utilities.helpers.trackman.data_source_factory import get_data_source

def main():
    print("\n" + "="*70)
    print("🎯 Trackman Data Integration - Interactive Test")
    print("="*70)

    # Get data source
    data_source = get_data_source()
    print(f"\n✅ Data Source: {type(data_source).__name__}")
    print(f"   Connection: {'PostgreSQL (localhost:5432)' if 'Redshift' in type(data_source).__name__ else 'Excel files'}")

    while True:
        print("\n" + "-"*70)
        print("Available queries:")
        print("  1. Show all errors (last 30 days)")
        print("  2. Show errors for FAC001")
        print("  3. Show errors for FAC002")
        print("  4. Show facility summary for FAC001")
        print("  5. Show facility summary for FAC002")
        print("  6. Show top error messages")
        print("  7. Show connectivity summary")
        print("  8. Quit")
        print("-"*70)

        choice = input("\nEnter your choice (1-8): ").strip()

        if choice == '8':
            print("\n👋 Goodbye!")
            break

        try:
            if choice == '1':
                print("\n📊 All Errors (last 30 days):")
                result = data_source.get_errors_summary(range_days=30)
                print(f"   Rows returned: {result.get('metadata', {}).get('rowCount', 0)}")
                for row in result.get('rows', []):
                    print(f"   Facility: {row[0]}, Count: {row[1]}, Unique Errors: {row[3]}")

            elif choice == '2':
                print("\n📊 Errors for FAC001:")
                result = data_source.get_errors_summary(range_days=30, facility_id="FAC001")
                print(f"   Rows returned: {result.get('metadata', {}).get('rowCount', 0)}")
                for row in result.get('rows', []):
                    print(f"   Count: {row[1]}, Unique Errors: {row[3]}")

            elif choice == '3':
                print("\n📊 Errors for FAC002:")
                result = data_source.get_errors_summary(range_days=30, facility_id="FAC002")
                print(f"   Rows returned: {result.get('metadata', {}).get('rowCount', 0)}")
                for row in result.get('rows', []):
                    print(f"   Count: {row[1]}, Unique Errors: {row[3]}")

            elif choice == '4':
                print("\n🏢 Facility Summary for FAC001:")
                result = data_source.get_facility_summary("FAC001", range_days=30)
                print(f"   Data: {result}")

            elif choice == '5':
                print("\n🏢 Facility Summary for FAC002:")
                result = data_source.get_facility_summary("FAC002", range_days=30)
                print(f"   Data: {result}")

            elif choice == '6':
                print("\n🔝 Top Error Messages:")
                result = data_source.get_top_error_messages(range_days=30, limit=10)
                print(f"   Rows returned: {result.get('metadata', {}).get('rowCount', 0)}")
                for row in result.get('rows', []):
                    print(f"   Error: {row[0]}, Message: {row[1]}, Count: {row[2]}")

            elif choice == '7':
                print("\n🔌 Connectivity Summary:")
                result = data_source.get_connectivity_summary(range_days=30)
                print(f"   Rows returned: {result.get('metadata', {}).get('rowCount', 0)}")
                for row in result.get('rows', []):
                    print(f"   Facility: {row[0]}, Online: {row[1]}, Offline: {row[2]}")

            else:
                print("\n❌ Invalid choice. Please try again.")

        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
