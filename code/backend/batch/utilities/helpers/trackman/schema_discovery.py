"""
Auto-discover database schema and generate configuration using AI.

This tool:
1. Connects to the database and introspects all tables/columns
2. Uses GPT to generate descriptions and keywords automatically
3. Writes the schema_config.yaml file

Usage:
    python -m backend.batch.utilities.helpers.trackman.schema_discovery

Or from code:
    from backend.batch.utilities.helpers.trackman.schema_discovery import discover_and_generate_schema
    discover_and_generate_schema()
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional

import yaml

logger = logging.getLogger(__name__)

# Output path for generated schema
SCHEMA_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "schema_config.yaml")


def get_database_connection():
    """Get database connection using environment variables."""
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST", os.getenv("POSTGRES_HOST", "localhost")),
        port=int(os.getenv("REDSHIFT_PORT", os.getenv("POSTGRES_PORT", "5432"))),
        database=os.getenv(
            "REDSHIFT_DATABASE", os.getenv("POSTGRES_DATABASE", "trackman")
        ),
        user=os.getenv("REDSHIFT_USER", os.getenv("POSTGRES_USER", "postgres")),
        password=os.getenv(
            "REDSHIFT_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")
        ),
    )


def introspect_database(
    schema_name: str = "public", exclude_patterns: List[str] = None
) -> Dict[str, Dict]:
    """
    Introspect database to discover all tables and columns.

    Args:
        schema_name: Database schema to introspect
        exclude_patterns: Table name patterns to exclude (e.g., ['_backup', '_temp'])

    Returns:
        Dict mapping table names to their column info
    """
    exclude_patterns = exclude_patterns or ["_backup", "_temp", "_old", "pg_", "sql_"]

    conn = get_database_connection()
    try:
        with conn.cursor() as cur:
            # Get all tables
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """,
                (schema_name,),
            )
            tables = [row[0] for row in cur.fetchall()]

            # Filter out excluded patterns
            tables = [
                t
                for t in tables
                if not any(pattern in t.lower() for pattern in exclude_patterns)
            ]

            # Get columns for each table
            schema_info = {}
            for table in tables:
                cur.execute(
                    """
                    SELECT
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                    ORDER BY ordinal_position
                """,
                    (schema_name, table),
                )

                columns = []
                column_types = {}
                for col_name, data_type, is_nullable, default in cur.fetchall():
                    columns.append(col_name)
                    column_types[col_name] = {
                        "type": data_type,
                        "nullable": is_nullable == "YES",
                    }

                # Get sample data for context
                try:
                    cur.execute(f'SELECT * FROM "{table}" LIMIT 3')
                    sample_rows = cur.fetchall()
                    sample_cols = [desc[0] for desc in cur.description]
                except Exception:
                    sample_rows = []
                    sample_cols = []

                # Get row count
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                    row_count = cur.fetchone()[0]
                except Exception:
                    row_count = 0

                schema_info[table] = {
                    "columns": columns,
                    "column_types": column_types,
                    "sample_data": {
                        "columns": sample_cols,
                        "rows": [list(row) for row in sample_rows[:2]],
                    },
                    "row_count": row_count,
                }

            logger.info(f"Discovered {len(schema_info)} tables")
            return schema_info

    finally:
        conn.close()


def generate_descriptions_with_ai(
    schema_info: Dict[str, Dict],
) -> Dict[str, Dict[str, Any]]:
    """
    Use GPT to generate descriptions, keywords, and metadata for tables.

    Args:
        schema_info: Raw schema info from introspection

    Returns:
        Enhanced schema with AI-generated descriptions
    """
    from openai import AzureOpenAI

    client = AzureOpenAI(
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-15-preview"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    )

    enhanced_schema = {}

    for table_name, table_info in schema_info.items():
        columns = table_info["columns"]
        column_types = table_info.get("column_types", {})
        sample_data = table_info.get("sample_data", {})
        row_count = table_info.get("row_count", 0)

        # Build context for AI
        columns_with_types = [
            f"{col} ({column_types.get(col, {}).get('type', 'unknown')})"
            for col in columns
        ]

        sample_preview = ""
        if sample_data.get("rows"):
            sample_preview = f"\nSample data: {json.dumps(sample_data['rows'][:1], default=str)[:500]}"

        prompt = f"""Analyze this database table and generate metadata:

Table: {table_name}
Columns: {', '.join(columns_with_types)}
Row count: {row_count:,}{sample_preview}

Generate a JSON response with:
1. "description": A concise 1-sentence description of what this table contains
2. "date_column": The column name used for time-based filtering (or null if none)
3. "keywords": List of 5-8 keywords users might use when asking about this data
4. "common_queries": List of 3 example natural language questions for this table
5. "category": One of: "errors", "connectivity", "usage", "metrics", "reference", "other"

Respond ONLY with valid JSON, no markdown."""

        try:
            response = client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_MODEL", "gpt-4"),
                messages=[
                    {
                        "role": "system",
                        "content": "You are a database analyst. Generate concise, accurate metadata for database tables.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
                max_tokens=500,
            )

            result_text = response.choices[0].message.content.strip()
            # Clean up potential markdown code blocks
            if result_text.startswith("```"):
                result_text = result_text.split("```")[1]
                if result_text.startswith("json"):
                    result_text = result_text[4:]
            result_text = result_text.strip()

            metadata = json.loads(result_text)

            enhanced_schema[table_name] = {
                "columns": columns,
                "description": metadata.get("description", ""),
                "date_column": metadata.get("date_column"),
                "keywords": metadata.get("keywords", []),
                "common_queries": metadata.get("common_queries", []),
                "category": metadata.get("category", "other"),
            }

            logger.info(f"Generated metadata for {table_name}")

        except Exception as e:
            logger.warning(f"AI generation failed for {table_name}: {e}")
            # Fallback to basic metadata
            enhanced_schema[table_name] = {
                "columns": columns,
                "description": f"Table containing {table_name.replace('_', ' ')} data",
                "date_column": _guess_date_column(columns),
                "keywords": _generate_basic_keywords(table_name, columns),
                "common_queries": [],
                "category": "other",
            }

    return enhanced_schema


def _guess_date_column(columns: List[str]) -> Optional[str]:
    """Guess the date column from column names."""
    date_patterns = [
        "timestamp",
        "created_at",
        "updated_at",
        "date",
        "_at",
        "_date",
        "_time",
    ]
    for col in columns:
        col_lower = col.lower()
        for pattern in date_patterns:
            if pattern in col_lower:
                return col
    return None


def _generate_basic_keywords(table_name: str, columns: List[str]) -> List[str]:
    """Generate basic keywords from table and column names."""
    keywords = set()

    # Add table name parts
    for part in table_name.split("_"):
        if len(part) > 2:
            keywords.add(part.lower())

    # Add significant column name parts
    for col in columns[:10]:
        for part in col.split("_"):
            if len(part) > 3 and part.lower() not in ["id", "name", "type", "count"]:
                keywords.add(part.lower())

    return list(keywords)[:8]


def generate_yaml_config(enhanced_schema: Dict[str, Dict]) -> str:
    """
    Generate the YAML configuration file content.

    Args:
        enhanced_schema: Schema with AI-generated metadata

    Returns:
        YAML string content
    """
    # Group tables by category
    categories = {}
    for table_name, table_info in enhanced_schema.items():
        category = table_info.get("category", "other")
        if category not in categories:
            categories[category] = []
        categories[category].append(table_name)

    # Build config structure
    config = {
        "tables": {},
        "aliases": {},
        "keywords": {},
    }

    # Add tables with metadata
    for table_name, table_info in enhanced_schema.items():
        config["tables"][table_name] = {
            "columns": table_info["columns"],
            "description": table_info.get("description", ""),
            "date_column": table_info.get("date_column"),
            "common_queries": table_info.get("common_queries", []),
        }

        # Add keywords
        if table_info.get("keywords"):
            config["keywords"][table_name] = table_info["keywords"]

    # Generate some sensible aliases
    for table_name in enhanced_schema:
        # Create short alias (e.g., error_logs -> errors)
        if table_name.endswith("_logs"):
            alias = table_name[:-5] + "s"
            config["aliases"][alias] = table_name
        elif table_name.endswith("_data"):
            alias = table_name[:-5]
            config["aliases"][alias] = table_name

    # Generate YAML with nice formatting
    yaml_content = """# Trackman Schema Configuration
# ================================
# Auto-generated by schema_discovery.py
# Run 'python -m backend.batch.utilities.helpers.trackman.schema_discovery' to regenerate
#
# To add a new table manually, add it under 'tables' section
# The system will validate queries against this allowlist

"""
    yaml_content += yaml.dump(config, default_flow_style=False, sort_keys=False)

    return yaml_content


def discover_and_generate_schema(
    schema_name: str = "public",
    use_ai: bool = True,
    output_path: str = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Main function to discover schema and generate configuration.

    Args:
        schema_name: Database schema to introspect
        use_ai: Whether to use AI for generating descriptions
        output_path: Path to write YAML file (default: schema_config.yaml)
        dry_run: If True, don't write file, just return config

    Returns:
        The generated configuration dict
    """
    output_path = output_path or SCHEMA_CONFIG_PATH

    print("🔍 Discovering database schema...")
    schema_info = introspect_database(schema_name)
    print(f"   Found {len(schema_info)} tables")

    if use_ai:
        print("🤖 Generating descriptions with AI...")
        enhanced_schema = generate_descriptions_with_ai(schema_info)
    else:
        print("📝 Generating basic metadata...")
        enhanced_schema = {}
        for table_name, table_info in schema_info.items():
            enhanced_schema[table_name] = {
                "columns": table_info["columns"],
                "description": f"Table: {table_name}",
                "date_column": _guess_date_column(table_info["columns"]),
                "keywords": _generate_basic_keywords(table_name, table_info["columns"]),
                "common_queries": [],
                "category": "other",
            }

    print("📄 Generating YAML configuration...")
    yaml_content = generate_yaml_config(enhanced_schema)

    if not dry_run:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(yaml_content)
        print(f"✅ Schema configuration written to: {output_path}")
    else:
        print("🔍 Dry run - configuration not written")
        print("\n" + yaml_content[:1000] + "...")

    return enhanced_schema


def refresh_schema():
    """
    Convenience function to refresh schema from database.
    Can be called periodically or on-demand.
    """
    try:
        discover_and_generate_schema(use_ai=True)
        # Clear the schema loader cache
        from .schema_loader import reload_schema_config

        reload_schema_config()
        logger.info("Schema refreshed successfully")
    except Exception as e:
        logger.error(f"Failed to refresh schema: {e}")
        raise


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Discover database schema and generate configuration")
    parser.add_argument("--schema", default="public", help="Database schema to introspect")
    parser.add_argument("--no-ai", action="store_true", help="Skip AI description generation")
    parser.add_argument("--dry-run", action="store_true", help="Don't write file, just preview")
    parser.add_argument("--output", help="Output file path")

    args = parser.parse_args()

    discover_and_generate_schema(
        schema_name=args.schema,
        use_ai=not args.no_ai,
        output_path=args.output,
        dry_run=args.dry_run,
    )
