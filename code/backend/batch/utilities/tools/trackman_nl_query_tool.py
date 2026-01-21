"""AI-powered natural language to SQL query tool for Trackman data.

Enhanced with Azure AI Foundry integration:
- Azure AI Tracing for observability
- Semantic caching for similar query matching
- Content Safety for SQL injection protection
"""

import hashlib
import logging
import time
from datetime import datetime
from typing import Dict, Optional

from ..common.answer import Answer
from ..helpers.llm_helper import LLMHelper
from ..helpers.trackman.data_source_factory import get_data_source
from ..helpers.trackman.redshift_config import (
    get_schema_for_prompt,
    get_sql_generation_prompt,
    validate_generated_sql,
    add_limit_if_missing,
)
from ..helpers.azure_ai_integration import (
    trace_operation,
    get_semantic_cache,
    get_content_safety_checker,
)

logger = logging.getLogger(__name__)


# ============================================================================
# SQL Cache - Tier 2 caching for generated SQL patterns
# ============================================================================
class SQLCache:
    """In-memory cache for SQL query patterns.

    Caches the mapping from normalized questions to generated SQL,
    reducing LLM calls for repeated or similar questions.
    """

    _instance = None
    _cache: Dict = {}
    _ttl: int = 86400 * 7  # 7 days
    _max_size: int = 1000  # Max cached queries

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _normalize_question(self, question: str) -> str:
        """Normalize question for consistent cache keys."""
        # Lowercase, strip whitespace, normalize spacing
        normalized = " ".join(question.lower().strip().split())
        # Replace specific numbers with placeholders for pattern matching
        # "last 7 days" and "last 10 days" become same pattern
        import re

        normalized = re.sub(r"\b\d+\b", "N", normalized)
        return normalized

    def _get_key(self, question: str) -> str:
        """Generate cache key from normalized question."""
        normalized = self._normalize_question(question)
        return hashlib.md5(normalized.encode()).hexdigest()

    def get(self, question: str) -> Optional[str]:
        """Get cached SQL for a question, or None if not cached/expired."""
        key = self._get_key(question)
        if key in self._cache:
            sql, timestamp, _ = self._cache[key]
            if time.time() - timestamp < self._ttl:
                logger.info("SQL cache HIT for question pattern")
                return sql
            else:
                # Expired, remove it
                del self._cache[key]
        return None

    def set(self, question: str, sql: str) -> None:
        """Cache SQL for a question pattern."""
        # Evict oldest entries if cache is full
        if len(self._cache) >= self._max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]

        key = self._get_key(question)
        self._cache[key] = (sql, time.time(), question)
        logger.info(f"SQL cached for question pattern (cache size: {len(self._cache)})")

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def stats(self) -> Dict:
        """Get cache statistics."""
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "ttl_days": self._ttl / 86400,
        }


# Global cache instance (legacy, kept for backward compatibility)
_sql_cache = SQLCache()


class TrackmanNLQueryTool:
    """Tool for querying Trackman data using natural language.

    Enhanced with Azure AI Foundry integration:
    - Semantic caching for intelligent query matching
    - Content Safety for SQL injection protection
    - Azure AI Tracing for observability

    This tool uses an LLM to generate SQL queries from natural language questions,
    validates them for safety, and executes them against the Trackman database.
    """

    def __init__(self, use_semantic_cache: bool = True):
        """
        Initialize the TrackmanNLQueryTool.

        Args:
            use_semantic_cache: Whether to use semantic (embedding-based) caching.
                              Falls back to pattern-based cache if False.
        """
        self.llm_helper = LLMHelper()
        self._use_semantic_cache = use_semantic_cache

        # Use semantic cache by default, with fallback to legacy cache
        if use_semantic_cache:
            self.semantic_cache = get_semantic_cache()
        self.legacy_cache = _sql_cache

        # Content safety checker
        self.content_safety = get_content_safety_checker()

    @trace_operation("sql_generation", {"tool": "trackman_nl_query"})
    def generate_sql_from_question(self, question: str, use_cache: bool = True) -> Dict:
        """
        Use LLM to generate SQL from natural language question.

        Enhanced with:
        - Semantic caching for similar query matching
        - Dynamic schema context from database introspection
        - Azure AI tracing for observability

        Args:
            question: Natural language question about Trackman data
            use_cache: Whether to use cached SQL if available (default: True)

        Returns:
            Dict with 'sql', 'success', 'prompt_tokens', 'completion_tokens', 'cached' keys
        """
        try:
            # Check semantic cache first (matches similar questions)
            if use_cache and self._use_semantic_cache:
                cached_sql = self.semantic_cache.get(question)
                if cached_sql:
                    return {
                        "sql": cached_sql,
                        "success": True,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "cached": True,
                        "cache_type": "semantic",
                    }

            # Fall back to legacy pattern-based cache
            if use_cache:
                cached_sql = self.legacy_cache.get(question)
                if cached_sql:
                    return {
                        "sql": cached_sql,
                        "success": True,
                        "prompt_tokens": 0,
                        "completion_tokens": 0,
                        "cached": True,
                        "cache_type": "pattern",
                    }

            # Get schema context - use smart retrieval based on question
            schema_context = get_schema_for_prompt(question)  # Smart schema based on keywords
            try:
                data_source = get_data_source()
                if hasattr(data_source, "get_dynamic_schema_for_prompt"):
                    dynamic_schema = data_source.get_dynamic_schema_for_prompt()
                    if dynamic_schema:
                        schema_context = dynamic_schema
                        logger.debug("Using dynamic schema for SQL generation")
            except Exception as e:
                logger.debug(f"Dynamic schema not available, using static: {e}")

            # Use configurable prompt with fallback to default
            base_prompt = get_sql_generation_prompt()
            system_prompt = base_prompt.format(
                schema=schema_context,
                current_date=datetime.now().strftime("%Y-%m-%d"),
            )

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question},
            ]

            response = self.llm_helper.get_chat_completion(messages)
            generated_sql = response.choices[0].message.content.strip()

            # Clean up markdown code blocks if present
            if generated_sql.startswith("```"):
                lines = generated_sql.split("\n")
                # Remove first line (```sql) and last line (```)
                generated_sql = "\n".join(
                    lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
                )

            generated_sql = generated_sql.strip().rstrip(";")

            logger.info(f"Generated SQL: {generated_sql}")

            # Cache the generated SQL in both caches for future use
            if self._use_semantic_cache:
                self.semantic_cache.set(question, generated_sql)
            self.legacy_cache.set(question, generated_sql)

            return {
                "sql": generated_sql,
                "success": True,
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "cached": False,
            }

        except Exception as e:
            logger.error(f"Error generating SQL: {str(e)}")
            return {
                "sql": "",
                "success": False,
                "error": str(e),
                "prompt_tokens": 0,
                "completion_tokens": 0,
            }

    @trace_operation("nl_query_execution", {"tool": "trackman_nl_query"})
    def query_with_natural_language(self, question: str, max_rows: int = 50) -> Answer:
        """
        Execute a natural language query against Trackman data.

        Enhanced pipeline with Azure AI Foundry integration:
        0. Check input with Content Safety (blocks injection attacks)
        1. Generate SQL from question using LLM (with semantic caching)
        2. Validate generated SQL for safety
        3. Add LIMIT if missing
        4. Execute query
        5. Format results as markdown

        Args:
            question: Natural language question
            max_rows: Maximum rows to return (default 50)

        Returns:
            Answer object with query results
        """
        try:
            # Step 0: Content Safety check for SQL injection attempts
            is_safe, safety_reason = self.content_safety.check_input(question)
            if not is_safe:
                logger.warning(f"Content safety blocked input: {safety_reason}")
                return Answer(
                    question=question,
                    answer="I'm sorry, but I can't process this request. "
                    "Please rephrase your question about Trackman data.",
                    source_documents=[],
                    prompt_tokens=0,
                    completion_tokens=0,
                )

            # Step 1: Generate SQL from question
            sql_result = self.generate_sql_from_question(question)

            if not sql_result["success"]:
                return Answer(
                    question=question,
                    answer=f"Failed to generate SQL query: {sql_result.get('error', 'Unknown error')}",
                    source_documents=[],
                    prompt_tokens=sql_result.get("prompt_tokens", 0),
                    completion_tokens=sql_result.get("completion_tokens", 0),
                )

            generated_sql = sql_result["sql"]

            # Step 2: Validate the generated SQL
            is_valid, error_message = validate_generated_sql(generated_sql)

            if not is_valid:
                return Answer(
                    question=question,
                    answer=f"Generated query failed validation: {error_message}\n\n"
                    f"Please try rephrasing your question or use a standard query type.",
                    source_documents=[],
                    prompt_tokens=sql_result.get("prompt_tokens", 0),
                    completion_tokens=sql_result.get("completion_tokens", 0),
                )

            # Step 3: Ensure LIMIT is present
            generated_sql = add_limit_if_missing(generated_sql, max_rows)

            # Step 4: Execute the query
            data_source = get_data_source()
            result = data_source.execute_custom_query(generated_sql)

            # Step 5: Format the result
            answer_text = self._format_result(result, question)

            return Answer(
                question=question,
                answer=answer_text,
                source_documents=[],
                prompt_tokens=sql_result.get("prompt_tokens", 0),
                completion_tokens=sql_result.get("completion_tokens", 0),
            )

        except Exception as e:
            logger.error(f"Error in natural language query: {str(e)}", exc_info=True)
            return Answer(
                question=question,
                answer=f"Error executing query: {str(e)}",
                source_documents=[],
            )

    def _format_result(self, result: Dict, question: str) -> str:
        """Format query result as markdown table."""
        columns = result.get("columns", [])
        rows = result.get("rows", [])
        metadata = result.get("metadata", {})

        if not rows:
            return "No data found for your query."

        # Build markdown table
        lines = [
            f"**Query Results** | Source: {metadata.get('source', 'unknown')} | Rows: {len(rows)}",
            "",
            "| " + " | ".join(str(col) for col in columns) + " |",
            "| " + " | ".join("---" for _ in columns) + " |",
        ]

        for row in rows[:50]:  # Limit display rows
            formatted_row = []
            for val in row:
                if val is None:
                    formatted_row.append("")
                else:
                    # Escape pipe characters in values
                    formatted_row.append(str(val).replace("|", "\\|"))
            lines.append("| " + " | ".join(formatted_row) + " |")

        if len(rows) > 50:
            lines.append(f"\n*Showing first 50 of {len(rows)} rows*")

        return "\n".join(lines)
