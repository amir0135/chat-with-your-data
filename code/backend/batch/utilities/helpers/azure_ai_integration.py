"""Azure AI Foundry integration for tracing, caching, and content safety.

This module provides:
- Azure AI Tracing for observability of LLM calls
- Semantic caching using Azure AI Search embeddings
- Content Safety integration for SQL injection protection
"""

import hashlib
import logging
import os
import time
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar

from azure.identity import DefaultAzureCredential
from azure.ai.contentsafety import ContentSafetyClient
from azure.ai.contentsafety.models import AnalyzeTextOptions

from .env_helper import EnvHelper

logger = logging.getLogger(__name__)


# ============================================================================
# Azure AI Tracing Configuration
# ============================================================================
def configure_azure_ai_tracing() -> bool:
    """
    Configure Azure AI tracing using OpenTelemetry.

    This enables end-to-end tracing of LLM calls, tool invocations,
    and database queries in Azure AI Foundry portal.

    Returns:
        bool: True if tracing was configured successfully
    """
    try:
        _ = EnvHelper()  # Validate environment is configured

        # Check if Application Insights is configured
        connection_string = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING", "")
        if not connection_string:
            logger.info("Application Insights not configured, tracing disabled")
            return False

        # Enable tracing for Azure AI SDK
        from azure.monitor.opentelemetry import configure_azure_monitor
        from opentelemetry import trace  # noqa: F401
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        # Configure Azure Monitor exporter
        configure_azure_monitor(
            connection_string=connection_string,
            enable_live_metrics=True,
        )

        # Instrument HTTP client for OpenAI calls
        HTTPXClientInstrumentor().instrument()

        logger.info("Azure AI tracing configured successfully")
        return True

    except ImportError as e:
        logger.warning(f"Tracing dependencies not installed: {e}")
        return False
    except Exception as e:
        logger.error(f"Failed to configure Azure AI tracing: {e}")
        return False


# Tracing decorator type
F = TypeVar("F", bound=Callable[..., Any])


def trace_operation(
    operation_name: str = "", attributes: Optional[Dict] = None
) -> Callable[[F], F]:
    """
    Decorator to trace function execution with Azure AI Foundry.

    Args:
        operation_name: Name for the traced span (defaults to function name)
        attributes: Additional attributes to attach to the span

    Usage:
        @trace_operation("sql_generation")
        def generate_sql(question: str) -> str:
            ...
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                from opentelemetry import trace as otel_trace

                tracer = otel_trace.get_tracer(__name__)
                span_name = operation_name or func.__name__

                with tracer.start_as_current_span(span_name) as span:
                    # Add custom attributes
                    if attributes:
                        for key, value in attributes.items():
                            span.set_attribute(key, str(value))

                    # Add function metadata
                    span.set_attribute("function.name", func.__name__)
                    span.set_attribute("function.module", func.__module__)

                    try:
                        result = func(*args, **kwargs)
                        span.set_attribute("status", "success")
                        return result
                    except Exception as e:
                        span.set_attribute("status", "error")
                        span.set_attribute("error.message", str(e))
                        span.record_exception(e)
                        raise

            except ImportError:
                # Tracing not available, execute without tracing
                return func(*args, **kwargs)

        return wrapper  # type: ignore

    return decorator


# ============================================================================
# Semantic Cache using Azure AI Search
# ============================================================================
class SemanticCache:
    """
    Semantic cache using Azure AI Search for similarity-based query matching.

    Instead of exact string matching, this cache finds semantically similar
    queries using embedding vectors, so "errors last week" matches
    "show errors from past 7 days".
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.env_helper = EnvHelper()
        self._cache: Dict[str, tuple] = {}  # Fallback in-memory cache
        self._embeddings_client = None
        self._search_client = None
        self._ttl = 86400 * 7  # 7 days
        self._similarity_threshold = 0.92  # High threshold for semantic matching
        self._initialized = True

        self._initialize_clients()

    def _initialize_clients(self):
        """Initialize Azure AI Search and embeddings clients."""
        try:
            from ..llm_helper import LLMHelper

            self._llm_helper = LLMHelper()

            # Check if semantic cache index exists
            self._index_name = os.getenv(
                "AZURE_SEARCH_SEMANTIC_CACHE_INDEX",
                f"{self.env_helper.AZURE_SEARCH_INDEX}-sql-cache",
            )

            logger.info(f"Semantic cache initialized with index: {self._index_name}")

        except Exception as e:
            logger.warning(f"Semantic cache initialization failed, using fallback: {e}")
            self._llm_helper = None

    def _get_embedding(self, text: str) -> Optional[List[float]]:
        """Generate embedding for text."""
        if not self._llm_helper:
            return None

        try:
            return self._llm_helper.generate_embeddings(text)
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return None

    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        import math

        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = math.sqrt(sum(a * a for a in vec1))
        norm2 = math.sqrt(sum(b * b for b in vec2))

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot_product / (norm1 * norm2)

    def get(self, question: str) -> Optional[str]:
        """
        Get cached SQL for a semantically similar question.

        Args:
            question: The natural language question

        Returns:
            Cached SQL if a similar question was found, None otherwise
        """
        question_embedding = self._get_embedding(question)

        if question_embedding is None:
            # Fall back to exact match
            key = hashlib.md5(question.lower().strip().encode()).hexdigest()
            if key in self._cache:
                sql, timestamp, _ = self._cache[key]
                if time.time() - timestamp < self._ttl:
                    logger.info("Semantic cache HIT (fallback exact match)")
                    return sql
            return None

        # Search for similar cached questions
        best_match = None
        best_similarity = 0.0

        for key, (
            sql,
            timestamp,
            cached_question,
            cached_embedding,
        ) in self._cache.items():
            if time.time() - timestamp >= self._ttl:
                continue

            if cached_embedding:
                similarity = self._cosine_similarity(
                    question_embedding, cached_embedding
                )
                if (
                    similarity > best_similarity
                    and similarity >= self._similarity_threshold
                ):
                    best_similarity = similarity
                    best_match = sql

        if best_match:
            logger.info(f"Semantic cache HIT (similarity: {best_similarity:.3f})")
            return best_match

        return None

    def set(self, question: str, sql: str) -> None:
        """
        Cache SQL with its semantic embedding.

        Args:
            question: The original natural language question
            sql: The generated SQL query
        """
        question_embedding = self._get_embedding(question)
        key = hashlib.md5(question.lower().strip().encode()).hexdigest()

        # Evict old entries if cache is full
        max_size = 1000
        if len(self._cache) >= max_size:
            # Remove oldest entries
            oldest_keys = sorted(self._cache.keys(), key=lambda k: self._cache[k][1])[
                :100
            ]
            for k in oldest_keys:
                del self._cache[k]

        self._cache[key] = (sql, time.time(), question, question_embedding)
        logger.info(f"Semantic cache SET (cache size: {len(self._cache)})")

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()
        logger.info("Semantic cache cleared")

    def stats(self) -> Dict:
        """Get cache statistics."""
        valid_entries = sum(
            1
            for _, (_, timestamp, _, _) in self._cache.items()
            if time.time() - timestamp < self._ttl
        )
        return {
            "total_entries": len(self._cache),
            "valid_entries": valid_entries,
            "ttl_days": self._ttl / 86400,
            "similarity_threshold": self._similarity_threshold,
        }


# Global semantic cache instance
_semantic_cache: Optional[SemanticCache] = None


def get_semantic_cache() -> SemanticCache:
    """Get or create the global semantic cache instance."""
    global _semantic_cache
    if _semantic_cache is None:
        _semantic_cache = SemanticCache()
    return _semantic_cache


# ============================================================================
# Content Safety Integration
# ============================================================================
class SQLContentSafetyChecker:
    """
    Content Safety checker for SQL generation to block injection attacks.

    Uses Azure Content Safety to detect and block:
    - Prompt injection attempts ("ignore previous instructions")
    - Malicious SQL patterns
    - Jailbreak attempts
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.env_helper = EnvHelper()
        self._client = None
        self._enabled = False
        self._initialized = True

        self._initialize_client()

    def _initialize_client(self):
        """Initialize Azure Content Safety client."""
        try:
            endpoint = os.getenv("AZURE_CONTENT_SAFETY_ENDPOINT", "")
            if not endpoint:
                logger.info("Content Safety endpoint not configured")
                return

            # Try key-based auth first, then RBAC
            key = os.getenv("AZURE_CONTENT_SAFETY_KEY", "")
            if key:
                from azure.core.credentials import AzureKeyCredential

                self._client = ContentSafetyClient(endpoint, AzureKeyCredential(key))
            else:
                credential = DefaultAzureCredential()
                self._client = ContentSafetyClient(endpoint, credential)

            self._enabled = True
            logger.info("Content Safety client initialized")

        except Exception as e:
            logger.warning(f"Content Safety initialization failed: {e}")
            self._enabled = False

    # Known SQL injection patterns to check locally
    _INJECTION_PATTERNS = [
        "ignore previous",
        "ignore all instructions",
        "forget your instructions",
        "disregard the above",
        "new instructions:",
        "system prompt:",
        "you are now",
        "act as if",
        "pretend you",
        "bypass",
        "override",
        "disable safety",
        "ignore safety",
        "drop table",
        "delete from",
        "truncate",
        "insert into",
        "update set",
        "grant all",
        "exec(",
        "execute(",
        "xp_cmdshell",
        "sp_executesql",
        "'; --",
        "' or '1'='1",
        "' or 1=1",
        "union select",
        "into outfile",
        "into dumpfile",
    ]

    def check_input(self, text: str) -> tuple[bool, Optional[str]]:
        """
        Check if input text is safe for SQL generation.

        Args:
            text: The user's question or input

        Returns:
            Tuple of (is_safe: bool, reason: Optional[str])
        """
        text_lower = text.lower()

        # First check local patterns (fast)
        for pattern in self._INJECTION_PATTERNS:
            if pattern in text_lower:
                logger.warning(f"SQL injection pattern detected: {pattern}")
                return False, "Input contains potentially dangerous pattern"

        # Then check with Azure Content Safety if available
        if self._enabled and self._client:
            try:
                request = AnalyzeTextOptions(text=text)
                response = self._client.analyze_text(request)

                # Check for jailbreak or harmful content
                for result in response.categories_analysis:
                    if result.severity >= 2:  # Medium or higher severity
                        logger.warning(
                            f"Content Safety flagged: {result.category} "
                            f"(severity: {result.severity})"
                        )
                        return False, "Content flagged as potentially harmful"

            except Exception as e:
                logger.error(f"Content Safety check failed: {e}")
                # Fail open - allow if service is unavailable
                # In production, you might want to fail closed instead

        return True, None

    def is_enabled(self) -> bool:
        """Check if Content Safety is enabled."""
        return self._enabled


# Global content safety checker instance
_content_safety_checker: Optional[SQLContentSafetyChecker] = None


def get_content_safety_checker() -> SQLContentSafetyChecker:
    """Get or create the global content safety checker instance."""
    global _content_safety_checker
    if _content_safety_checker is None:
        _content_safety_checker = SQLContentSafetyChecker()
    return _content_safety_checker


# ============================================================================
# Schema Introspection Cache
# ============================================================================
class SchemaCache:
    """
    Cache for database schema introspection results.

    Stores discovered table/column information with TTL to avoid
    repeated introspection queries.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._schema: Optional[Dict] = None
            cls._instance._sample_data: Optional[Dict] = None
            cls._instance._timestamp: float = 0
            cls._instance._ttl: int = 3600  # 1 hour default
        return cls._instance

    def get_schema(self) -> Optional[Dict]:
        """Get cached schema if still valid."""
        if self._schema and time.time() - self._timestamp < self._ttl:
            return self._schema
        return None

    def get_sample_data(self) -> Optional[Dict]:
        """Get cached sample data if still valid."""
        if self._sample_data and time.time() - self._timestamp < self._ttl:
            return self._sample_data
        return None

    def set_schema(self, schema: Dict, sample_data: Optional[Dict] = None) -> None:
        """Cache schema and optionally sample data."""
        self._schema = schema
        self._sample_data = sample_data
        self._timestamp = time.time()
        logger.info(f"Schema cache updated with {len(schema)} tables")

    def invalidate(self) -> None:
        """Invalidate the cache."""
        self._schema = None
        self._sample_data = None
        self._timestamp = 0
        logger.info("Schema cache invalidated")

    def is_valid(self) -> bool:
        """Check if cache is still valid."""
        return self._schema is not None and time.time() - self._timestamp < self._ttl

    def set_ttl(self, seconds: int) -> None:
        """Set cache TTL in seconds."""
        self._ttl = seconds


# Global schema cache instance
_schema_cache: Optional[SchemaCache] = None


def get_schema_cache() -> SchemaCache:
    """Get or create the global schema cache instance."""
    global _schema_cache
    if _schema_cache is None:
        _schema_cache = SchemaCache()
    return _schema_cache
