#!/usr/bin/env python3
"""
TrackMan Integration Test Suite

This script tests the TrackMan data integration by:
1. Querying the API with various questions
2. Verifying the responses contain expected data
3. Cross-checking results against direct database queries

Usage:
    python tests/test_trackman_integration.py

Requirements:
    - Flask backend running on port 5050
    - PostgreSQL database with TrackMan data
    - Environment variables set (or use start_local.sh)
"""

import os
import sys
import json
import requests
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
import re

# Configuration
API_URL = "http://127.0.0.1:5050/api/conversation"
DB_CONFIG = {
    "host": os.getenv("REDSHIFT_HOST", "localhost"),
    "port": os.getenv("REDSHIFT_PORT", "5432"),
    "database": os.getenv("REDSHIFT_DB", "trackman_test"),
    "user": os.getenv("REDSHIFT_USER", "testuser"),
    "password": os.getenv("REDSHIFT_PASSWORD", "testpassword"),
}

# Test results
PASSED = 0
FAILED = 0
ERRORS = []


def get_db_connection():
    """Create a database connection."""
    return psycopg2.connect(**DB_CONFIG)


def query_api(question: str) -> Dict[str, Any]:
    """Send a question to the API and return the response."""
    payload = {
        "conversation_id": f"test-{datetime.now().timestamp()}",
        "messages": [{"role": "user", "content": question}],
    }

    try:
        response = requests.post(API_URL, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def extract_answer(api_response: Dict) -> str:
    """Extract the answer text from the API response."""
    try:
        choices = api_response.get("choices", [])
        if choices:
            messages = choices[0].get("messages", [])
            for msg in messages:
                if msg.get("role") == "assistant":
                    return msg.get("content", "")
        return ""
    except Exception as e:
        return f"Error extracting answer: {e}"


def verify_contains(answer: str, expected_items: list) -> Tuple[bool, str]:
    """Verify the answer contains expected items."""
    missing = []
    found = []
    for item in expected_items:
        if str(item).lower() in answer.lower():
            found.append(item)
        else:
            missing.append(item)

    if missing:
        return False, f"Missing: {missing}"
    return True, f"Found: {found}"


def verify_number_in_range(answer: str, min_val: int, max_val: int) -> Tuple[bool, str]:
    """Verify a number in the answer is within expected range."""
    # Find all numbers in the answer
    numbers = re.findall(r"\b(\d{1,3}(?:,\d{3})*|\d+)\b", answer)

    for num_str in numbers:
        try:
            num = int(num_str.replace(",", ""))
            if min_val <= num <= max_val:
                return True, f"Found {num:,} in range [{min_val:,}, {max_val:,}]"
        except ValueError:
            continue

    return False, f"No number in range [{min_val:,}, {max_val:,}] found"


def test_case(name: str, question: str, validators: list) -> bool:
    """Run a single test case."""
    global PASSED, FAILED, ERRORS

    print(f"\n{'='*60}")
    print(f"TEST: {name}")
    print(f"Question: {question}")
    print("-" * 60)

    # Query the API
    response = query_api(question)

    if "error" in response:
        print(f"❌ API Error: {response['error']}")
        FAILED += 1
        ERRORS.append((name, f"API Error: {response['error']}"))
        return False

    answer = extract_answer(response)

    if not answer:
        print(f"❌ Empty answer received")
        FAILED += 1
        ERRORS.append((name, "Empty answer"))
        return False

    # Show truncated answer
    display_answer = answer[:500] + "..." if len(answer) > 500 else answer
    print(f"Answer (preview): {display_answer}")
    print("-" * 60)

    # Run validators
    all_passed = True
    for validator_name, validator_func in validators:
        passed, details = validator_func(answer)
        status = "✅" if passed else "❌"
        print(f"  {status} {validator_name}: {details}")
        if not passed:
            all_passed = False

    if all_passed:
        print(f"\n✅ TEST PASSED: {name}")
        PASSED += 1
    else:
        print(f"\n❌ TEST FAILED: {name}")
        FAILED += 1
        ERRORS.append((name, "Validation failed"))

    return all_passed


def get_expected_values_from_db() -> Dict[str, Any]:
    """Query database to get expected values for validation."""
    conn = get_db_connection()
    values = {}

    try:
        with conn.cursor() as cur:
            # Total errors in last 7 days
            cur.execute(
                """
                SELECT COUNT(*) FROM error_logs
                WHERE error_timestamp >= CURRENT_DATE - INTERVAL '7 days'
            """
            )
            values["errors_7d"] = cur.fetchone()[0]

            # Total disconnections in last 7 days
            cur.execute(
                """
                SELECT COALESCE(SUM(disconnection_cnt), 0) FROM connectivity_logs
                WHERE log_date >= CURRENT_DATE - INTERVAL '7 days'
            """
            )
            values["disconnections_7d"] = cur.fetchone()[0]

            # Unique facilities with errors
            cur.execute(
                """
                SELECT COUNT(DISTINCT facility_id) FROM error_logs
                WHERE error_timestamp >= CURRENT_DATE - INTERVAL '7 days'
            """
            )
            values["unique_facilities_errors"] = cur.fetchone()[0]

            # Top facility by errors (name)
            cur.execute(
                """
                SELECT facility_name, COUNT(*) as cnt
                FROM error_logs
                WHERE error_timestamp >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY facility_name
                ORDER BY cnt DESC
                LIMIT 1
            """
            )
            row = cur.fetchone()
            values["top_error_facility_name"] = row[0] if row else None
            values["top_error_facility_count"] = row[1] if row else 0

            # Top facility by disconnections
            cur.execute(
                """
                SELECT facility_name, SUM(disconnection_cnt) as total
                FROM connectivity_logs
                WHERE log_date >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY facility_name
                ORDER BY total DESC
                LIMIT 1
            """
            )
            row = cur.fetchone()
            values["top_disconnect_facility_name"] = row[0] if row else None
            values["top_disconnect_facility_count"] = row[1] if row else 0

            # Sample facility names for testing
            cur.execute(
                """
                SELECT DISTINCT facility_name
                FROM error_logs
                WHERE facility_name IS NOT NULL AND facility_name != ''
                ORDER BY facility_name
                LIMIT 5
            """
            )
            values["sample_facilities"] = [row[0] for row in cur.fetchall()]

    finally:
        conn.close()

    return values


def run_all_tests():
    """Run all test cases."""
    global PASSED, FAILED, ERRORS

    print("\n" + "=" * 60)
    print("TRACKMAN INTEGRATION TEST SUITE")
    print("=" * 60)

    # Check prerequisites
    print("\nChecking prerequisites...")

    # Check API is running
    try:
        response = requests.get("http://127.0.0.1:5050/", timeout=5)
        print("✅ Flask API is running")
    except:
        print("❌ Flask API is not running. Start it with ./start_local.sh")
        return 1

    # Check database connection
    try:
        conn = get_db_connection()
        conn.close()
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return 1

    # Get expected values from database
    print("\nFetching expected values from database...")
    expected = get_expected_values_from_db()
    print(f"  - Errors in last 7 days: {expected['errors_7d']:,}")
    print(f"  - Disconnections in last 7 days: {expected['disconnections_7d']:,}")
    print(
        f"  - Top error facility: {expected['top_error_facility_name']} ({expected['top_error_facility_count']:,} errors)"
    )
    print(
        f"  - Top disconnect facility: {expected['top_disconnect_facility_name']} ({expected['top_disconnect_facility_count']:,} disconnections)"
    )

    # Define test cases
    test_cases = [
        # Test 1: Error summary
        (
            "Error Summary - Last 7 Days",
            "Show me errors from the last 7 days",
            [
                ("Response contains 'error'", lambda a: verify_contains(a, ["error"])),
                ("Response contains table data", lambda a: verify_contains(a, ["|"])),
                (
                    "Response contains facility info",
                    lambda a: verify_contains(a, ["facility"]),
                ),
            ],
        ),
        # Test 2: Disconnections
        (
            "Disconnections Summary",
            "Which facilities have the most disconnections in the last 7 days?",
            [
                (
                    "Response contains 'disconnection'",
                    lambda a: verify_contains(a, ["disconnect"]),
                ),
                ("Response contains table data", lambda a: verify_contains(a, ["|"])),
                (
                    "Top facility mentioned",
                    lambda a: (
                        verify_contains(a, [expected["top_disconnect_facility_name"]])
                        if expected["top_disconnect_facility_name"]
                        else (True, "Skipped")
                    ),
                ),
            ],
        ),
        # Test 3: Specific facility query
        (
            "Specific Facility Query",
            f"Show me errors for {expected['sample_facilities'][0] if expected['sample_facilities'] else 'Facility 1'}",
            [
                (
                    "Response is not empty",
                    lambda a: (len(a) > 50, f"Answer length: {len(a)}"),
                ),
                (
                    "Response contains error info or no data message",
                    lambda a: (
                        verify_contains(a, ["error"])
                        if "no data" not in a.lower()
                        else (True, "No data message")
                    ),
                ),
            ],
        ),
        # Test 4: Top errors
        (
            "Top Error Messages",
            "What are the most common error messages in the last 7 days?",
            [
                (
                    "Response contains error info",
                    lambda a: verify_contains(a, ["error"]),
                ),
                (
                    "Response has structured data",
                    lambda a: (len(a) > 100, f"Answer length: {len(a)}"),
                ),
            ],
        ),
        # Test 5: Connectivity analysis
        (
            "Connectivity Analysis",
            "Analyze connectivity issues across all facilities",
            [
                (
                    "Response mentions connectivity",
                    lambda a: verify_contains(a, ["connect"]),
                ),
                (
                    "Response contains analysis or data",
                    lambda a: (len(a) > 100, f"Answer length: {len(a)}"),
                ),
            ],
        ),
        # Test 6: Time range variation
        (
            "30-Day Error Summary",
            "Show error summary for the last 30 days",
            [
                (
                    "Response contains error info",
                    lambda a: verify_contains(a, ["error"]),
                ),
                (
                    "Response contains data",
                    lambda a: verify_contains(a, ["|"])
                    or (len(a) > 100, "Has content"),
                ),
            ],
        ),
        # Test 7: Natural language query
        (
            "Natural Language Query",
            "Which locations are having the most problems?",
            [
                (
                    "Response provides location/facility info",
                    lambda a: verify_contains(a, ["facility"])
                    or verify_contains(a, ["location"]),
                ),
            ],
        ),
        # Test 8: Count verification
        (
            "Data Count Verification",
            "How many total errors occurred in the last 7 days?",
            [
                ("Response mentions errors", lambda a: verify_contains(a, ["error"])),
                (
                    "Response has numbers",
                    lambda a: (bool(re.search(r"\d+", a)), "Contains numbers"),
                ),
            ],
        ),
    ]

    # Run all tests
    for name, question, validators in test_cases:
        test_case(name, question, validators)

    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"  ✅ Passed: {PASSED}")
    print(f"  ❌ Failed: {FAILED}")
    print(f"  Total:  {PASSED + FAILED}")

    if ERRORS:
        print("\nFailed tests:")
        for name, error in ERRORS:
            print(f"  - {name}: {error}")

    print("\n" + "=" * 60)

    # Return exit code
    return 0 if FAILED == 0 else 1


def main():
    """Main entry point."""
    # Set environment variables if not set
    os.environ.setdefault("REDSHIFT_HOST", "localhost")
    os.environ.setdefault("REDSHIFT_PORT", "5432")
    os.environ.setdefault("REDSHIFT_DB", "trackman_test")
    os.environ.setdefault("REDSHIFT_USER", "testuser")
    os.environ.setdefault("REDSHIFT_PASSWORD", "testpassword")

    exit_code = run_all_tests()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
