#!/usr/bin/env python3
"""
Interactive TrackMan Question Tester

Tests all the TrackMan questions and displays results in a clean format.
"""

import requests
import json
import time
import sys

API_URL = "http://127.0.0.1:5050/api/conversation"

# All test questions organized by category
TEST_QUESTIONS = {
    "Error Analysis": [
        "Show me errors from the last 7 days",
        "What are the most common error messages?",
        "How many total errors occurred in the last 7 days?",
        "Show error summary for the last 30 days",
    ],
    "Connectivity Issues": [
        "Which facilities have the most disconnections?",
        "Show me the top 10 facilities by disconnections this week",
        "Show connectivity logs for the last week",
    ],
    "Facility-Specific Queries": [
        "Show me errors for Five Iron Golf",
        "What's the error count for DSG Global?",
        "Show disconnections for PGA Tour Superstore",
    ],
    "Natural Language Queries": [
        "Which locations are having the most problems?",
        "What facilities need attention?",
        "Show me the worst performing sites",
    ],
    "Combined Queries": [
        "Show facilities with both high errors and high disconnections",
        "Which facilities have critical errors in the last 7 days?",
    ],
}


def query_api(question: str) -> dict:
    """Send a question to the API."""
    payload = {
        "conversation_id": f"test-{time.time()}",
        "messages": [{"role": "user", "content": question}]
    }
    try:
        response = requests.post(API_URL, json=payload, timeout=120)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {"error": str(e)}


def extract_answer(response: dict) -> str:
    """Extract answer text from API response."""
    try:
        choices = response.get("choices", [])
        if choices:
            messages = choices[0].get("messages", [])
            for msg in messages:
                if msg.get("role") == "assistant":
                    return msg.get("content", "")
        return "No answer found"
    except:
        return f"Error: {response.get('error', 'Unknown error')}"


def print_divider(char="=", width=80):
    print(char * width)


def run_tests():
    """Run all tests interactively."""
    print_divider()
    print("🔍 TRACKMAN QUESTION TESTER")
    print_divider()

    # Check API
    print("\nChecking API connection...")
    try:
        requests.get("http://127.0.0.1:5050/", timeout=5)
        print("✅ API is accessible\n")
    except:
        print("❌ API not accessible. Make sure Flask is running on port 5050")
        return

    total = sum(len(q) for q in TEST_QUESTIONS.values())
    current = 0
    results = []

    for category, questions in TEST_QUESTIONS.items():
        print_divider("-")
        print(f"📁 {category}")
        print_divider("-")

        for question in questions:
            current += 1
            print(f"\n[{current}/{total}] 💬 {question}")
            print("    ⏳ Querying...")

            start = time.time()
            response = query_api(question)
            elapsed = time.time() - start

            answer = extract_answer(response)

            # Truncate long answers for display
            display = answer[:500] + "..." if len(answer) > 500 else answer

            if "error" in response:
                print(f"    ❌ Error: {response['error']}")
                results.append((question, False, elapsed))
            else:
                print(f"    ✅ Response ({elapsed:.1f}s, {len(answer)} chars):")
                # Indent the answer
                for line in display.split('\n')[:15]:
                    print(f"       {line}")
                if len(answer) > 500 or len(display.split('\n')) > 15:
                    print("       [... truncated ...]")
                results.append((question, True, elapsed))

            print()

    # Summary
    print_divider()
    print("📊 SUMMARY")
    print_divider()

    passed = sum(1 for _, success, _ in results if success)
    failed = total - passed
    avg_time = sum(t for _, _, t in results) / len(results)

    print(f"  ✅ Passed: {passed}/{total}")
    print(f"  ❌ Failed: {failed}/{total}")
    print(f"  ⏱️  Avg response time: {avg_time:.1f}s")
    print_divider()


if __name__ == "__main__":
    run_tests()
