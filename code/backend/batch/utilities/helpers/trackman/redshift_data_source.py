"""Redshift-based Trackman data source implementation."""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Any, List

import psycopg2
from psycopg2 import sql

from .data_source_interface import TrackmanDataSource
from .redshift_config import validate_table, get_allowed_columns

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
            f"Initialized Redshift data source: {self.user}@{self.host}:{self.port}/{self.database}"
        )

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
        self, query: sql.SQL, params: tuple = None
    ) -> List[List[Any]]:
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
                columns = [desc[0] for desc in cur.description]
                return {"columns": columns, "rows": rows}
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
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

    def get_errors_summary(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """Get summary of errors within the specified time range."""
        try:
            self._validate_table_access("errors")

            # Build parameterized query
            cutoff_date = self._get_date_filter(range_days)

            if facility_id:
                query = sql.SQL(
                    """
                    SELECT
                        facility_id,
                        COUNT(*) as error_count,
                        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_count,
                        COUNT(DISTINCT error_code) as unique_errors
                    FROM {schema}.{table}
                    WHERE timestamp >= %s AND facility_id = %s
                    GROUP BY facility_id
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("errors"),
                )
                params = (cutoff_date, facility_id)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        facility_id,
                        COUNT(*) as error_count,
                        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical_count,
                        COUNT(DISTINCT error_code) as unique_errors
                    FROM {schema}.{table}
                    WHERE timestamp >= %s
                    GROUP BY facility_id
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("errors"),
                )
                params = (cutoff_date,)

            result = self._execute_query(query, params)
            return self._format_result(
                result, {"range_days": range_days, "facility_id": facility_id}
            )
        except Exception as e:
            logger.error(f"Error in get_errors_summary: {str(e)}")
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
                        error_message,
                        error_code,
                        COUNT(*) as count,
                        MAX(severity) as severity
                    FROM {schema}.{table}
                    WHERE timestamp >= %s AND facility_id = %s
                    GROUP BY error_message, error_code
                    ORDER BY count DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("errors"),
                )
                params = (cutoff_date, facility_id, limit)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        error_message,
                        error_code,
                        COUNT(*) as count,
                        MAX(severity) as severity
                    FROM {schema}.{table}
                    WHERE timestamp >= %s
                    GROUP BY error_message, error_code
                    ORDER BY count DESC
                    LIMIT %s
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("errors"),
                )
                params = (cutoff_date, limit)

            result = self._execute_query(query, params)
            return self._format_result(
                result,
                {"range_days": range_days, "limit": limit, "facility_id": facility_id},
            )
        except Exception as e:
            logger.error(f"Error in get_top_error_messages: {str(e)}")
            raise

    def get_connectivity_summary(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """Get connectivity status summary."""
        try:
            self._validate_table_access("connectivity")

            cutoff_date = self._get_date_filter(range_days)

            if facility_id:
                query = sql.SQL(
                    """
                    SELECT
                        facility_id,
                        COUNT(*) as total_events,
                        SUM(CASE WHEN connectivity_status = 'connected' THEN 1 ELSE 0 END) as connected_count,
                        ROUND(100.0 * SUM(CASE WHEN connectivity_status = 'connected' THEN 1 ELSE 0 END) / COUNT(*), 2) as connected_pct
                    FROM {schema}.{table}
                    WHERE timestamp >= %s AND facility_id = %s
                    GROUP BY facility_id
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("connectivity"),
                )
                params = (cutoff_date, facility_id)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        facility_id,
                        COUNT(*) as total_events,
                        SUM(CASE WHEN connectivity_status = 'connected' THEN 1 ELSE 0 END) as connected_count,
                        ROUND(100.0 * SUM(CASE WHEN connectivity_status = 'connected' THEN 1 ELSE 0 END) / COUNT(*), 2) as connected_pct
                    FROM {schema}.{table}
                    WHERE timestamp >= %s
                    GROUP BY facility_id
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("connectivity"),
                )
                params = (cutoff_date,)

            result = self._execute_query(query, params)
            return self._format_result(
                result, {"range_days": range_days, "facility_id": facility_id}
            )
        except Exception as e:
            logger.error(f"Error in get_connectivity_summary: {str(e)}")
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
                        disconnect_reason,
                        COUNT(*) as count,
                        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
                    FROM {schema}.{table}
                    WHERE timestamp >= %s
                        AND connectivity_status = 'disconnected'
                        AND facility_id = %s
                    GROUP BY disconnect_reason
                    ORDER BY count DESC
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("connectivity"),
                )
                params = (cutoff_date, facility_id)
            else:
                query = sql.SQL(
                    """
                    SELECT
                        disconnect_reason,
                        COUNT(*) as count,
                        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
                    FROM {schema}.{table}
                    WHERE timestamp >= %s
                        AND connectivity_status = 'disconnected'
                    GROUP BY disconnect_reason
                    ORDER BY count DESC
                    """
                ).format(
                    schema=sql.Identifier(self.schema),
                    table=sql.Identifier("connectivity"),
                )
                params = (cutoff_date,)

            result = self._execute_query(query, params)
            return self._format_result(
                result, {"range_days": range_days, "facility_id": facility_id}
            )
        except Exception as e:
            logger.error(f"Error in get_disconnect_reasons: {str(e)}")
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
        except Exception as e:
            logger.error(f"Error in get_facility_summary: {str(e)}")
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
        except Exception as e:
            logger.error(f"Error in get_data_quality_summary: {str(e)}")
            raise
