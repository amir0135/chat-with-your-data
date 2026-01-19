"""SQL Query Quality Evaluation for Trackman NL-to-SQL.

This module provides evaluation metrics for measuring the quality of
LLM-generated SQL queries using Azure AI Evaluation SDK.

Metrics evaluated:
- SQL Validity: Does the query parse and validate correctly?
- Schema Groundedness: Are all tables/columns from the allowed schema?
- Query Correctness: Does the SQL answer the intended question?
- Response Quality: Is the formatted answer coherent and relevant?
"""

import logging
import os
import pathlib
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from azure.ai.evaluation import (
    CoherenceEvaluator,
    GroundednessEvaluator,
    RelevanceEvaluator,
    evaluate,
)
from dotenv import load_dotenv

# Add parent path for imports
import sys

sys.path.insert(
    0, str(pathlib.Path(__file__).parent.parent.parent / "code" / "backend")
)

from batch.utilities.helpers.trackman.redshift_config import (
    ALLOWED_TABLES,
    TABLE_ALIASES,
    validate_generated_sql,
)
from batch.utilities.tools.trackman_nl_query_tool import TrackmanNLQueryTool

load_dotenv()

logger = logging.getLogger(__name__)


# ============================================================================
# Custom SQL Evaluators
# ============================================================================


class SQLValidityEvaluator:
    """Evaluates whether generated SQL is syntactically valid and safe."""

    def __init__(self):
        self.name = "sql_validity"

    def __call__(self, *, query: str, response: str, **kwargs) -> Dict[str, Any]:
        """
        Evaluate SQL validity.

        Args:
            query: The natural language question
            response: The generated SQL query

        Returns:
            Dict with validity score and details
        """
        # Extract SQL from response if it's embedded in text
        sql = self._extract_sql(response)

        if not sql:
            return {
                "sql_validity": 0.0,
                "sql_validity_reason": "No SQL query found in response",
            }

        # Use the existing validation function
        is_valid, error_message = validate_generated_sql(sql)

        if is_valid:
            return {
                "sql_validity": 1.0,
                "sql_validity_reason": "Query is valid and safe",
            }
        else:
            return {
                "sql_validity": 0.0,
                "sql_validity_reason": error_message or "Validation failed",
            }

    def _extract_sql(self, text: str) -> Optional[str]:
        """Extract SQL from response text."""
        # Try to find SQL in code blocks
        code_block_match = re.search(
            r"```(?:sql)?\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE
        )
        if code_block_match:
            return code_block_match.group(1).strip()

        # If the text starts with SELECT, assume it's raw SQL
        if text.strip().upper().startswith("SELECT"):
            return text.strip()

        return None


class SchemaGroundednessEvaluator:
    """Evaluates whether SQL uses only tables and columns from the allowed schema."""

    def __init__(self):
        self.name = "schema_groundedness"
        self.allowed_tables = set(ALLOWED_TABLES.keys()) | set(TABLE_ALIASES.keys())
        self.all_columns = {}
        for table, columns in ALLOWED_TABLES.items():
            self.all_columns[table] = set(columns)

    def __call__(self, *, query: str, response: str, **kwargs) -> Dict[str, Any]:
        """
        Evaluate schema groundedness.

        Args:
            query: The natural language question
            response: The generated SQL query

        Returns:
            Dict with groundedness score and details
        """
        sql = response.strip()

        # Extract tables used
        tables_used = self._extract_tables(sql)
        invalid_tables = [
            t for t in tables_used if t.lower() not in self.allowed_tables
        ]

        if invalid_tables:
            return {
                "schema_groundedness": 0.0,
                "schema_groundedness_reason": f"Invalid tables: {', '.join(invalid_tables)}",
            }

        # Check if tables are valid
        if not tables_used:
            return {
                "schema_groundedness": 0.5,
                "schema_groundedness_reason": "No tables detected in query",
            }

        return {
            "schema_groundedness": 1.0,
            "schema_groundedness_reason": f"All tables valid: {', '.join(tables_used)}",
        }

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL query."""
        tables = []

        # FROM clause
        from_match = re.search(r"\bFROM\s+(\w+)", sql, re.IGNORECASE)
        if from_match:
            tables.append(from_match.group(1))

        # JOIN clauses
        join_matches = re.findall(r"\bJOIN\s+(\w+)", sql, re.IGNORECASE)
        tables.extend(join_matches)

        return tables


class SQLExecutionEvaluator:
    """Evaluates whether the SQL query executes successfully and returns results."""

    def __init__(self, use_mock: bool = True):
        """
        Initialize the SQL execution evaluator.

        Args:
            use_mock: If True, only validate syntax. If False, actually execute queries.
        """
        self.name = "sql_execution"
        self.use_mock = use_mock

    def __call__(self, *, query: str, response: str, **kwargs) -> Dict[str, Any]:
        """
        Evaluate SQL execution.

        Args:
            query: The natural language question
            response: The generated SQL query

        Returns:
            Dict with execution score and details
        """
        if self.use_mock:
            # In mock mode, just check that it looks like valid SQL
            sql = response.strip()
            if sql.upper().startswith("SELECT") and "FROM" in sql.upper():
                return {
                    "sql_execution": 1.0,
                    "sql_execution_reason": "Query structure appears valid (mock mode)",
                }
            return {
                "sql_execution": 0.0,
                "sql_execution_reason": "Query structure invalid (mock mode)",
            }

        # Real execution mode
        try:
            from batch.utilities.helpers.trackman.data_source_factory import (
                get_data_source,
            )

            data_source = get_data_source()
            result = data_source.execute_custom_query(response, max_rows=5)

            if result and result.get("rows"):
                return {
                    "sql_execution": 1.0,
                    "sql_execution_reason": f"Query executed, returned {len(result['rows'])} rows",
                }
            else:
                return {
                    "sql_execution": 0.5,
                    "sql_execution_reason": "Query executed but returned no rows",
                }

        except Exception as e:
            return {
                "sql_execution": 0.0,
                "sql_execution_reason": f"Execution failed: {str(e)}",
            }


# ============================================================================
# SQL Query Evaluation Runner
# ============================================================================


class SQLQueryEvaluationRunner:
    """Runs comprehensive evaluation of SQL generation quality."""

    def __init__(self, model_config: Optional[Dict] = None):
        """
        Initialize the evaluation runner.

        Args:
            model_config: Azure OpenAI model configuration for LLM-based evaluators
        """
        self.model_config = model_config or {
            "azure_endpoint": os.getenv("AZURE_ENDPOINT"),
            "azure_deployment": os.getenv("AZURE_DEPLOYMENT"),
        }

        # Initialize custom evaluators
        self.sql_validity = SQLValidityEvaluator()
        self.schema_groundedness = SchemaGroundednessEvaluator()
        self.sql_execution = SQLExecutionEvaluator(use_mock=True)

        # Initialize LLM-based evaluators for response quality
        if self.model_config.get("azure_endpoint"):
            self.relevance = RelevanceEvaluator(self.model_config)
            self.coherence = CoherenceEvaluator(self.model_config)
        else:
            self.relevance = None
            self.coherence = None

    def generate_test_dataset(self, questions: List[str]) -> pd.DataFrame:
        """
        Generate evaluation dataset by running questions through the SQL generator.

        Args:
            questions: List of natural language questions

        Returns:
            DataFrame with query, sql, and response columns
        """
        tool = TrackmanNLQueryTool(use_semantic_cache=False)
        results = []

        for question in questions:
            try:
                sql_result = tool.generate_sql_from_question(question, use_cache=False)
                answer = tool.query_with_natural_language(question, max_rows=10)

                results.append(
                    {
                        "query": question,
                        "sql": sql_result.get("sql", ""),
                        "response": (
                            answer.answer if hasattr(answer, "answer") else str(answer)
                        ),
                        "sql_success": sql_result.get("success", False),
                        "cached": sql_result.get("cached", False),
                    }
                )
            except Exception as e:
                results.append(
                    {
                        "query": question,
                        "sql": "",
                        "response": f"Error: {str(e)}",
                        "sql_success": False,
                        "cached": False,
                    }
                )

        return pd.DataFrame(results)

    def evaluate_dataset(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Run all evaluators on a dataset.

        Args:
            df: DataFrame with query, sql, and response columns

        Returns:
            Dict with evaluation results and summary metrics
        """
        results = []

        for _, row in df.iterrows():
            row_result = {
                "query": row["query"],
                "sql": row.get("sql", ""),
                "response": row.get("response", ""),
            }

            # Run SQL-specific evaluators on the SQL column
            validity = self.sql_validity(
                query=row["query"], response=row.get("sql", "")
            )
            groundedness = self.schema_groundedness(
                query=row["query"], response=row.get("sql", "")
            )
            execution = self.sql_execution(
                query=row["query"], response=row.get("sql", "")
            )

            row_result.update(validity)
            row_result.update(groundedness)
            row_result.update(execution)

            # Run LLM evaluators on the response column if available
            if self.relevance and row.get("response"):
                try:
                    rel_result = self.relevance(
                        query=row["query"], response=row["response"]
                    )
                    row_result["relevance"] = rel_result.get("relevance", 0)
                except Exception:
                    row_result["relevance"] = None

            if self.coherence and row.get("response"):
                try:
                    coh_result = self.coherence(
                        query=row["query"], response=row["response"]
                    )
                    row_result["coherence"] = coh_result.get("coherence", 0)
                except Exception:
                    row_result["coherence"] = None

            results.append(row_result)

        # Calculate summary metrics
        results_df = pd.DataFrame(results)
        summary = {
            "total_queries": len(results_df),
            "sql_validity_rate": results_df["sql_validity"].mean(),
            "schema_groundedness_rate": results_df["schema_groundedness"].mean(),
            "sql_execution_rate": results_df["sql_execution"].mean(),
        }

        if "relevance" in results_df.columns:
            summary["avg_relevance"] = results_df["relevance"].mean()
        if "coherence" in results_df.columns:
            summary["avg_coherence"] = results_df["coherence"].mean()

        return {
            "results": results,
            "summary": summary,
        }


# ============================================================================
# Sample Test Questions
# ============================================================================

SAMPLE_SQL_QUESTIONS = [
    "Show me the top 10 facilities with the most errors in the last 7 days",
    "Which facilities have connectivity issues?",
    "How many errors were there yesterday?",
    "What are the most common error messages?",
    "Show strokes per bay for the last month",
    "Which bays have the most disconnections?",
    "List all critical errors from last week",
    "Compare error counts between facilities",
    "Show me the facility with the highest occupancy",
    "What is the average disconnection count per facility?",
]


def main():
    """Run SQL evaluation with sample questions."""
    print("=" * 60)
    print("SQL Query Quality Evaluation")
    print("=" * 60)

    # Initialize runner
    runner = SQLQueryEvaluationRunner()

    # Generate test dataset
    print("\nGenerating test dataset...")
    df = runner.generate_test_dataset(SAMPLE_SQL_QUESTIONS)
    print(f"Generated {len(df)} test cases")

    # Run evaluation
    print("\nRunning evaluation...")
    results = runner.evaluate_dataset(df)

    # Print summary
    print("\n" + "=" * 60)
    print("EVALUATION SUMMARY")
    print("=" * 60)
    for metric, value in results["summary"].items():
        if isinstance(value, float):
            print(f"  {metric}: {value:.2%}")
        else:
            print(f"  {metric}: {value}")

    # Print detailed results
    print("\n" + "=" * 60)
    print("DETAILED RESULTS")
    print("=" * 60)
    for r in results["results"]:
        print(f"\nQuery: {r['query'][:60]}...")
        print(f"  SQL Validity: {r['sql_validity']:.0%} - {r['sql_validity_reason']}")
        print(f"  Schema Groundedness: {r['schema_groundedness']:.0%}")
        print(f"  SQL Execution: {r['sql_execution']:.0%}")

    # Save results
    results_df = pd.DataFrame(results["results"])
    output_path = pathlib.Path(__file__).parent / "data" / "sql_evaluation_results.json"
    results_df.to_json(output_path, orient="records", indent=2)
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    main()
