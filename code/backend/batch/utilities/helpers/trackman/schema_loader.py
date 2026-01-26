"""
Dynamic schema configuration loader for Trackman.

Loads table/column configuration from YAML file, making it easy to
add new tables without touching Python code.
"""

import os
import logging
from typing import Dict, List, Optional, Any
from functools import lru_cache

import yaml

logger = logging.getLogger(__name__)

# Path to the schema config file
SCHEMA_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "schema_config.yaml")


@lru_cache(maxsize=1)
def _load_schema_config() -> Dict[str, Any]:
    """Load and cache the schema configuration from YAML."""
    try:
        with open(SCHEMA_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            logger.info(
                "Loaded schema config with %d tables",
                len(config.get("tables", {})),
            )
            return config
    except FileNotFoundError:
        logger.warning("Schema config file not found: %s", SCHEMA_CONFIG_PATH)
        return {"tables": {}, "aliases": {}, "keywords": {}}
    except yaml.YAMLError as e:
        logger.error("Error parsing schema config: %s", e)
        return {"tables": {}, "aliases": {}, "keywords": {}}


def reload_schema_config() -> None:
    """Force reload of schema configuration."""
    _load_schema_config.cache_clear()
    _load_schema_config()
    logger.info("Schema configuration reloaded")


def get_allowed_tables() -> Dict[str, List[str]]:
    """
    Get mapping of table names to their allowed columns.

    Returns:
        Dict[str, List[str]]: Table name -> list of column names
    """
    config = _load_schema_config()
    tables = config.get("tables", {})
    return {
        table_name: table_info.get("columns", [])
        for table_name, table_info in tables.items()
    }


def get_table_aliases() -> Dict[str, str]:
    """Get mapping of table aliases to actual table names."""
    config = _load_schema_config()
    return config.get("aliases", {})


def get_table_keywords() -> Dict[str, List[str]]:
    """Get mapping of table names to their associated keywords."""
    config = _load_schema_config()
    return config.get("keywords", {})


def get_table_description(table_name: str) -> str:
    """Get description for a table."""
    config = _load_schema_config()
    tables = config.get("tables", {})
    return tables.get(table_name, {}).get("description", "")


def get_date_column(table_name: str) -> Optional[str]:
    """Get the date column for time-based filtering."""
    config = _load_schema_config()
    tables = config.get("tables", {})
    return tables.get(table_name, {}).get("date_column")


def get_common_queries(table_name: str) -> List[str]:
    """Get example queries for a table."""
    config = _load_schema_config()
    tables = config.get("tables", {})
    return tables.get(table_name, {}).get("common_queries", [])


def resolve_table_name(table_name: str) -> str:
    """Resolve a table alias to its actual name."""
    aliases = get_table_aliases()
    return aliases.get(table_name.lower(), table_name)


def validate_table(table_name: str) -> bool:
    """Check if a table is in the allowlist."""
    resolved = resolve_table_name(table_name)
    allowed = get_allowed_tables()
    return resolved in allowed


def validate_columns(table_name: str, columns: List[str]) -> bool:
    """Validate that all columns are allowed for the table."""
    resolved = resolve_table_name(table_name)
    allowed_tables = get_allowed_tables()

    if resolved not in allowed_tables:
        return False

    allowed_cols = set(allowed_tables[resolved])
    requested = set(columns)
    return requested.issubset(allowed_cols)


def get_relevant_tables(question: str) -> List[str]:
    """
    Determine relevant tables based on question keywords.

    Args:
        question: Natural language question

    Returns:
        List of relevant table names
    """
    question_lower = question.lower()
    keywords = get_table_keywords()
    relevant = set()

    for table, table_keywords in keywords.items():
        for kw in table_keywords:
            if kw in question_lower:
                relevant.add(table)
                break

    # If no match, return all tables
    if not relevant:
        return list(get_allowed_tables().keys())

    return list(relevant)


def generate_schema_prompt(question: Optional[str] = None) -> str:
    """
    Generate schema description for LLM prompt.

    Args:
        question: Optional question to filter relevant tables

    Returns:
        Formatted schema description
    """
    allowed_tables = get_allowed_tables()

    if question:
        tables_to_include = get_relevant_tables(question)
    else:
        tables_to_include = list(allowed_tables.keys())

    lines = ["## Available Tables and Columns\n"]

    for table in tables_to_include:
        columns = allowed_tables.get(table, [])
        description = get_table_description(table)
        date_col = get_date_column(table)

        lines.append(f"### {table}")
        if description:
            lines.append(f"*{description}*")

        lines.append(f"**Columns:** {', '.join(columns)}")

        if date_col:
            lines.append(f"**Date column for filtering:** `{date_col}`")

        common = get_common_queries(table)
        if common:
            lines.append("**Example queries:**")
            for q in common[:3]:
                lines.append(f"  - {q}")

        lines.append("")

    return "\n".join(lines)


# Convenience exports for backward compatibility
def get_schema_for_prompt(question: str = None) -> str:
    """Backward-compatible wrapper for generate_schema_prompt."""
    return generate_schema_prompt(question)
