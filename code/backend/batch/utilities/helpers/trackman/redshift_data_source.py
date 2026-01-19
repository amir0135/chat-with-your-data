"""Redshift-based Trackman data source implementation."""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List

import psycopg2
from psycopg2 import sql

from .data_source_interface import TrackmanDataSource
from .redshift_config import validate_table, ALLOWED_TABLES
from ..azure_ai_integration import get_schema_cache, trace_operation

logger = logging.getLogger(__name__)


class RedshiftDataSource(TrackmanDataSource):
    """Redshift-based implementation of Trackman data source."""

    def __init__(self):
        """Initialize Redshift data source using environment variables."""
        self.host = os.getenv("REDSHIFT_HOST")
        self.port = os.getenv("REDSHIFT_PORT", "5439")
        self.database = os.getenv("REDSHIFT_DB")
        self.user = os.getenv("REDSHIFT_USER")
        self.password = os.getenv("REDSHIFT_PASSWORD")
        self.schema = os.getenv("REDSHIFT_SCHEMA", "public")

        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError(
                "Missing required Redshift environment variables: "
                "REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD"
            )

        logger.info(
            "Initialized Redshift data source: %s@%s:%s/%s",
            self.user,
            self.host,
            self.port,
            self.database,
        )

        # Initialize schema introspection on startup
        self._introspect_schema_if_needed()

    def _introspect_schema_if_needed(self) -> None:
        """Introspect database schema if not cached."""
        schema_cache = get_schema_cache()
        if schema_cache.is_valid():
            logger.debug("Using cached schema")
            return

        try:
            schema_info = self._introspect_schema()
            sample_data = self._get_sample_data(list(schema_info.keys()))
            schema_cache.set_schema(schema_info, sample_data)
        except (psycopg2.Error, OSError) as e:
            logger.warning("Schema introspection failed, using static schema: %s", e)

    @trace_operation("schema_introspection")
    def _introspect_schema(self) -> Dict[str, Dict]:
        """
        Query information_schema to discover table structure dynamically.

        Returns:
            Dict mapping table names to their column information
        """
        # Only introspect tables in our allowlist for security
        allowed_tables = list(ALLOWED_TABLES.keys())

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                # Query column information
                cur.execute(
                    """
                    SELECT
                        table_name,
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = ANY(%s)
                    ORDER BY table_name, ordinal_position
                """,
                    (self.schema, allowed_tables),
                )

                rows = cur.fetchall()

                schema_info: Dict[str, Dict] = {}
                for (
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                ) in rows:
                    if table_name not in schema_info:
                        schema_info[table_name] = {
                            "columns": {},
                            "description": "",
                        }
                    schema_info[table_name]["columns"][column_name] = {
                        "type": data_type,
                        "nullable": is_nullable == "YES",
                        "default": column_default,
                    }

                logger.info("Introspected schema for %d tables", len(schema_info))
                return schema_info

        except psycopg2.Error as e:
            logger.error("Error introspecting schema: %s", e)
            raise
        finally:
            if conn:
                conn.close()

    @trace_operation("sample_data_retrieval")
    def _get_sample_data(
        self, tables: List[str], sample_size: int = 3
    ) -> Dict[str, Dict[str, Any]]:
        """
        Fetch sample rows from each table to help LLM understand data patterns.

        Args:
            tables: List of table names to sample
            sample_size: Number of sample rows per table (default: 3)

        Returns:
            Dict mapping table names to sample rows with columns and rows
        """
        sample_data: Dict[str, Dict[str, Any]] = {}

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                for table_name in tables:
                    if table_name not in ALLOWED_TABLES:
                        continue

                    try:
                        # Use parameterized identifier for table name
                        query = sql.SQL(
                            "SELECT * FROM {schema}.{table} LIMIT %s"
                        ).format(
                            schema=sql.Identifier(self.schema),
                            table=sql.Identifier(table_name),
                        )
                        cur.execute(query, (sample_size,))

                        columns = (
                            [desc[0] for desc in cur.description]
                            if cur.description
                            else []
                        )
                        rows = cur.fetchall()

                        sample_data[table_name] = {
                            "columns": columns,
                            "rows": [list(row) for row in rows],
                        }

                    except psycopg2.Error as e:
                        logger.warning("Error sampling table %s: %s", table_name, e)
                        sample_data[table_name] = {"columns": [], "rows": []}

            logger.info("Retrieved sample data for %d tables", len(sample_data))
            return sample_data

        except psycopg2.Error as e:
            logger.error("Error getting sample data: %s", e)
            return {}
        finally:
            if conn:
                conn.close()

    def get_dynamic_schema_for_prompt(self) -> str:
        """
        Generate dynamic schema description for LLM prompt.

        This combines introspected schema with sample data to give the LLM
        rich context about actual table structure and data patterns.

        Returns:
            Formatted schema string for inclusion in SQL generation prompt
        """
        schema_cache = get_schema_cache()
        schema_info = schema_cache.get_schema()
        sample_data = schema_cache.get_sample_data()

        if not schema_info:
            # Fall back to static schema
            from .redshift_config import get_schema_for_prompt

            return get_schema_for_prompt()

        lines = ["## Available Tables and Columns\n"]

        for table_name, table_info in schema_info.items():
            lines.append(f"### {table_name}")

            # Add column info with types
            columns = table_info.get("columns", {})
            if columns:
                lines.append("**Columns:**")
                for col_name, col_info in columns.items():
                    col_type = col_info.get("type", "unknown")
                    nullable = "(nullable)" if col_info.get("nullable") else ""
                    lines.append(f"  - `{col_name}`: {col_type} {nullable}")

            # Add sample data if available
            if sample_data and table_name in sample_data:
                samples = sample_data[table_name]
                if samples.get("rows"):
                    lines.append("\n**Sample data:**")
                    sample_cols = samples.get("columns", [])
                    lines.append(
                        "| " + " | ".join(sample_cols[:6]) + " |"
                    )  # Limit columns
                    lines.append(
                        "| " + " | ".join(["---"] * min(6, len(sample_cols))) + " |"
                    )

                    for row in samples.get("rows", [])[:2]:  # Show 2 rows max
                        # Truncate long values and limit columns
                        formatted_row = []
                        for val in row[:6]:
                            str_val = str(val) if val is not None else ""
                            if len(str_val) > 30:
                                str_val = str_val[:27] + "..."
                            formatted_row.append(str_val.replace("|", "\\|"))
                        lines.append("| " + " | ".join(formatted_row) + " |")

            lines.append("")

        return "\n".join(lines)

    def refresh_schema_cache(self) -> None:
        """Force refresh of the schema cache."""
        schema_cache = get_schema_cache()
        schema_cache.invalidate()
        self._introspect_schema_if_needed()
        logger.info("Schema cache refreshed")

    def _get_connection(self):
        """Create a connection to Redshift."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

    def _execute_query(
        self, query: sql.Composable, params: Optional[tuple] = None
    ) -> Dict[str, Any]:
        """
        Execute a parameterized query safely.

        Args:
            query: sql.SQL composed query object
            params: Query parameters

        Returns:
            List of rows
        """
        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                rows = cur.fetchall()
                columns = (
                    [desc[0] for desc in cur.description] if cur.description else []
                )
                return {"columns": columns, "rows": rows}
        except psycopg2.Error as e:
            logger.error("Error executing query: %s", e)
            raise
        finally:
            if conn:
                conn.close()

    def _validate_table_access(self, table_name: str):
        """Validate table is in allowlist."""
        if not validate_table(table_name):
            raise ValueError(f"Table '{table_name}' is not in the allowlist")

    def _format_result(
        self, query_result: Dict, metadata: Dict, source: str = "redshift"
    ) -> Dict:
        """Format query result as standard dict."""
        return {
            "columns": query_result["columns"],
            "rows": query_result["rows"],
            "metadata": {
                **metadata,
                "source": source,
                "rowCount": len(query_result["rows"]),
            },
        }

    def _get_date_filter(self, range_days: int) -> datetime:
        """Get cutoff datetime for date filtering."""
        return datetime.now() - timedelta(days=range_days)

    def _resolve_facility(self, facility: Optional[str]) -> tuple:
        """
        Resolve a facility identifier to (facility_id, facility_name).
        Accepts either a UUID or a facility name.
        Returns (facility_id, facility_name) or (None, None) if not found.
        """
        if not facility:
            return None, None

        # Check if it looks like a UUID
        is_uuid = len(facility) == 36 and facility.count("-") == 4

        conn = None
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                if is_uuid:
                    # Search by facility_id
                    cur.execute(
                        "SELECT facility_id, facility_name FROM error_logs WHERE facility_id = %s LIMIT 1",
                        (facility,),
                    )
                else:
                    # Search by facility_name (case-insensitive)
                    cur.execute(
                        "SELECT facility_id, facility_name FROM error_logs WHERE LOWER(facility_name) = LOWER(%s) LIMIT 1",
                        (facility,),
                    )
                row = cur.fetchone()
                if row:
                    return row[0], row[1]
                return None, None
        except psycopg2.Error as e:
            logger.error("Error resolving facility: %s", e)
            return None, None
        finally:
            if conn:
                conn.close()

    def get_errors_summary(
        self, range_days: int, facility_id: Optional[str] = None, max_rows: int = 100
    ) -> Dict:
        """Get summary of errors within the specified time range."""
        try:
            self._validate_table_access("errors")

            # Build parameterized query
            cutoff_date = self._get_date_filter(range_days)

            # Resolve facility name to ID if provided
            resolved_id, _ = self._resolve_facility(facility_id)
            if facility_id and not resolved_id:
                logger.warning("Facility not found: %s", facility_id)

            if resolved_id:
                query = sql.SQL(
                    """
                    SELECT
                        facility_name,
                        facility_id,
                        COUNT(*) as error_count,
                        SUM(CASE WHEN UPPER(event_level) = 'ERROR' THEN 1 ELSE 0 END) as critical_count,
                        COUNT(DISTINCT message_id) as unique_errors
                    FROM {schema}.{table}
                    WHERE error_timestamp >= %s AND facility_id = %s
                    GROUP BY facility_name, facility_id
                    ORDER BY error_count DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("error_logs"),
                )
                params = (cutoff_date, resolved_id, max_rows)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        facility_name,
                        facility_id,
                        COUNT(*) as error_count,
                        SUM(CASE WHEN UPPER(event_level) = 'ERROR' THEN 1 ELSE 0 END) as critical_count,
                        COUNT(DISTINCT message_id) as unique_errors
                    FROM {schema}.{table}
                    WHERE error_timestamp >= %s
                    GROUP BY facility_name, facility_id
                    ORDER BY error_count DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("error_logs"),
                )
                params = (cutoff_date, max_rows)

            result = self._execute_query(query, params)
            truncated = len(result["rows"]) >= max_rows
            return self._format_result(
                result,
                {
                    "range_days": range_days,
                    "facility_id": facility_id,
                    "max_rows": max_rows,
                    "truncated": truncated,
                },
            )
        except (psycopg2.Error, ValueError) as e:
            logger.error("Error in get_errors_summary: %s", e)
            raise

    def get_top_error_messages(
        self, range_days: int, limit: int = 10, facility_id: Optional[str] = None
    ) -> Dict:
        """Get top error messages by frequency."""
        try:
            self._validate_table_access("errors")

            cutoff_date = self._get_date_filter(range_days)

            if facility_id:
                query = sql.SQL(
                    """
                    SELECT
                        message,
                        message_id,
                        COUNT(*) as count,
                        MAX(event_level) as severity
                    FROM {schema}.{table}
                    WHERE error_timestamp >= %s AND facility_id = %s
                    GROUP BY message, message_id
                    ORDER BY count DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("error_logs"),
                )
                params = (cutoff_date, facility_id, limit)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        message,
                        message_id,
                        COUNT(*) as count,
                        MAX(event_level) as severity
                    FROM {schema}.{table}
                    WHERE error_timestamp >= %s
                    GROUP BY message, message_id
                    ORDER BY count DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("error_logs"),
                )
                params = (cutoff_date, limit)

            result = self._execute_query(query, params)
            return self._format_result(
                result,
                {"range_days": range_days, "limit": limit, "facility_id": facility_id},
            )
        except (psycopg2.Error, ValueError) as e:
            logger.error("Error in get_top_error_messages: %s", e)
            raise

    def get_connectivity_summary(
        self, range_days: int, facility_id: Optional[str] = None, max_rows: int = 100
    ) -> Dict:
        """Get connectivity status summary."""
        try:
            self._validate_table_access("connectivity")

            cutoff_date = self._get_date_filter(range_days)

            # Resolve facility name to ID if provided
            resolved_id, _ = self._resolve_facility(facility_id)
            if facility_id and not resolved_id:
                logger.warning("Facility not found: %s", facility_id)

            if resolved_id:
                query = sql.SQL(
                    """
                    SELECT
                        facility_name,
                        facility_id,
                        COUNT(*) as total_events,
                        SUM(disconnection_cnt) as total_disconnections,
                        ROUND(AVG(disconnection_cnt), 2) as avg_disconnections
                    FROM {schema}.{table}
                    WHERE log_date >= %s AND facility_id = %s
                    GROUP BY facility_name, facility_id
                    ORDER BY total_disconnections DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("connectivity_logs"),
                )
                params = (cutoff_date, resolved_id, max_rows)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        facility_name,
                        facility_id,
                        COUNT(*) as total_events,
                        SUM(disconnection_cnt) as total_disconnections,
                        ROUND(AVG(disconnection_cnt), 2) as avg_disconnections
                    FROM {schema}.{table}
                    WHERE log_date >= %s
                    GROUP BY facility_name, facility_id
                    ORDER BY total_disconnections DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("connectivity_logs"),
                )
                params = (cutoff_date, max_rows)

            result = self._execute_query(query, params)
            truncated = len(result["rows"]) >= max_rows
            return self._format_result(
                result,
                {
                    "range_days": range_days,
                    "facility_id": facility_id,
                    "max_rows": max_rows,
                    "truncated": truncated,
                },
            )
        except (psycopg2.Error, ValueError) as e:
            logger.error("Error in get_connectivity_summary: %s", e)
            raise

    def get_disconnect_reasons(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """Get disconnect reasons breakdown."""
        try:
            self._validate_table_access("connectivity")

            cutoff_date = self._get_date_filter(range_days)

            if facility_id:
                query = sql.SQL(
                    """
                    SELECT
                        model_status,
                        SUM(disconnection_cnt) as count
                    FROM {schema}.{table}
                    WHERE log_date >= %s
                        AND disconnection_cnt > 0
                        AND facility_id = %s
                    GROUP BY model_status
                    ORDER BY count DESC
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("connectivity_logs"),
                )
                params = (cutoff_date, facility_id)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        model_status,
                        SUM(disconnection_cnt) as count
                    FROM {schema}.{table}
                    WHERE log_date >= %s
                        AND disconnection_cnt > 0
                    GROUP BY model_status
                    ORDER BY count DESC
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("connectivity_logs"),
                )
                params = (cutoff_date,)

            result = self._execute_query(query, params)
            return self._format_result(
                result, {"range_days": range_days, "facility_id": facility_id}
            )
        except (psycopg2.Error, ValueError) as e:
            logger.error("Error in get_disconnect_reasons: %s", e)
            raise

    def get_facility_summary(self, facility_id: str, range_days: int) -> Dict:
        """Get comprehensive summary for a specific facility."""
        try:
            self._validate_table_access("facility_metadata")

            # Get facility metadata
            meta_query = sql.SQL(
                """
                SELECT
                    'location' as metric, location as value FROM {schema}.facility_metadata WHERE facility_id = %s
                UNION ALL
                SELECT
                    'opening_hours', opening_hours FROM {schema}.facility_metadata WHERE facility_id = %s
                UNION ALL
                SELECT
                    'subscription_status', subscription_status FROM {schema}.facility_metadata WHERE facility_id = %s
                UNION ALL
                SELECT
                    'units_deployed', CAST(units_deployed AS VARCHAR) FROM {schema}.facility_metadata WHERE facility_id = %s
                UNION ALL
                SELECT
                    'usage_hours_30d', CAST(usage_hours_30d AS VARCHAR) FROM {schema}.facility_metadata WHERE facility_id = %s
                UNION ALL
                SELECT
                    'strokes_tracked', CAST(strokes_tracked AS VARCHAR) FROM {schema}.facility_metadata WHERE facility_id = %s
                UNION ALL
                SELECT
                    'tournaments_hosted', CAST(tournaments_hosted AS VARCHAR) FROM {schema}.facility_metadata WHERE facility_id = %s
                """
            ).format(schema=sql.Identifier(self.schema))

            params = (facility_id,) * 7
            result = self._execute_query(meta_query, params)

            # Add recent metrics
            cutoff_date = self._get_date_filter(range_days)

            # Errors count
            self._validate_table_access("errors")
            error_query = sql.SQL(
                """
                SELECT
                    'errors_total' as metric,
                    CAST(COUNT(*) AS VARCHAR) as value
                FROM {schema}.errors
                WHERE facility_id = %s AND timestamp >= %s
                UNION ALL
                SELECT
                    'errors_critical',
                    CAST(SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS VARCHAR)
                FROM {schema}.errors
                WHERE facility_id = %s AND timestamp >= %s
                """
            ).format(schema=sql.Identifier(self.schema))

            error_result = self._execute_query(
                error_query, (facility_id, cutoff_date, facility_id, cutoff_date)
            )

            # Combine results
            result["rows"].extend(error_result["rows"])

            return self._format_result(
                result, {"facility_id": facility_id, "range_days": range_days}
            )
        except (psycopg2.Error, ValueError) as e:
            logger.error("Error in get_facility_summary: %s", e)
            raise

    def get_data_quality_summary(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """Get data quality metrics summary."""
        try:
            self._validate_table_access("data_quality")

            cutoff_date = self._get_date_filter(range_days)

            if facility_id:
                query = sql.SQL(
                    """
                    SELECT
                        facility_id,
                        ROUND(AVG(data_quality_score), 2) as avg_quality_score,
                        SUM(missing_records) as total_missing_records,
                        ROUND(AVG(latency_ms), 2) as avg_latency_ms
                    FROM {schema}.{table}
                    WHERE timestamp >= %s AND facility_id = %s
                    GROUP BY facility_id
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("data_quality"),
                )
                params = (cutoff_date, facility_id)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        facility_id,
                        ROUND(AVG(data_quality_score), 2) as avg_quality_score,
                        SUM(missing_records) as total_missing_records,
                        ROUND(AVG(latency_ms), 2) as avg_latency_ms
                    FROM {schema}.{table}
                    WHERE timestamp >= %s
                    GROUP BY facility_id
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("data_quality"),
                )
                params = (cutoff_date,)

            result = self._execute_query(query, params)
            return self._format_result(
                result, {"range_days": range_days, "facility_id": facility_id}
            )
        except (psycopg2.Error, ValueError) as e:
            logger.error("Error in get_data_quality_summary: %s", e)
            raise

    def execute_custom_query(self, sql_query: str, max_rows: int = 100) -> Dict:
        """
        Execute a validated custom SQL query.

        This method runs dynamically generated SQL queries from the AI tool.
        IMPORTANT: The query should be validated before calling this method.

        Args:
            sql_query: A validated SQL SELECT query
            max_rows: Maximum number of rows to return (default 100)

        Returns:
            Dict with columns, rows, and metadata
        """
        conn = None
        try:
            # Enforce LIMIT if not already present
            query_upper = sql_query.upper().strip()
            if "LIMIT" not in query_upper:
                sql_query = f"{sql_query.rstrip().rstrip(';')} LIMIT {max_rows}"

            logger.info("Executing custom query: %s...", sql_query[:200])

            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(sql_query)
                rows = cur.fetchall()
                columns = (
                    [desc[0] for desc in cur.description] if cur.description else []
                )

            truncated = len(rows) >= max_rows
            return {
                "columns": columns,
                "rows": [list(row) for row in rows],
                "metadata": {
                    "source": "redshift",
                    "rowCount": len(rows),
                    "truncated": truncated,
                    "max_rows": max_rows,
                    "query_type": "custom",
                },
            }
        except psycopg2.Error as e:
            logger.error("Error executing custom query: %s", e)
            raise
        finally:
            if conn:
                conn.close()
