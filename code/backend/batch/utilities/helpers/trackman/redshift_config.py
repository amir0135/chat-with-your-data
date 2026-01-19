"""Configuration for Redshift/PostgreSQL table and column allowlists.

This matches the actual Trackman data schema from CSV exports.
"""

# Allowlist configuration matching actual CSV data
# Only these tables and columns can be queried

ALLOWED_TABLES = {
    "sessions": [
        "id",
        "sg_facility_id",
        "sg_bay_id",
        "activity_date",
        "facility",
        "bay",
        "radar_serial",
        "earliest_occurrence",
        "latest_occurrence",
        "session_count",
        "strokes_total",
        "ballspeed_cnt_total",
        "clubspeed_cnt_total",
        "clubspeed_pickup_total",
        "strokes_shotanalysis",
        "clubspeed_cnt_shotanalysis",
        "clubspeed_pickup_shotanalysis",
        "strokes_vgrange",
        "clubspeed_cnt_vgrange",
        "clubspeed_pickup_vgrange",
        "strokes_courseplay",
        "clubspeed_cnt_courseplay",
        "clubspeed_pickup_courseplay",
        "tps_version",
        "iorelease",
    ],
    "connectivity_logs": [
        "id",
        "facility_id",
        "facility_name",
        "bay_id",
        "bay_name",
        "log_date",
        "disconnection_cnt",
        "model_status",
        "client_version",
        "radar_serial",
        "radar_firmware",
        "connection_type",
        "speed_mbps",
        "mode",
        "device_root",
        "cpu_freeze",
    ],
    "error_logs": [
        "id",
        "error_timestamp",
        "facility_id",
        "facility_name",
        "bay_id",
        "radar_serial",
        "radar_firmware",
        "radar_model",
        "tps_messages_amount",
        "event_level",
        "message_id",
        "message",
        "facility",
        "bay",
    ],
    "indoor_kpis": [
        "id",
        "facility_name",
        "facility_id",
        "bay_name",
        "bay_id",
        "is_bay_still_configured",
        "does_bay_have_activity",
        "timezone",
        "visible_on_locator",
        "earliest_last_bay_activity",
        "latest_last_bay_activity",
        "latest_configured_unit_activity",
        "occupancy",
        "configured_bays",
        "active_units",
        "total_strokes",
        "strokes_per_bay",
        "logged_in_players_per_bay",
        "logged_in_players_w_app",
        "logged_in_players_active_days_ratio",
    ],
}

# Table aliases for backward compatibility
TABLE_ALIASES = {
    "errors": "error_logs",
    "connectivity": "connectivity_logs",
    "connections": "connectivity_logs",
    "tps_messages": "error_logs",
    "facility_kpis": "indoor_kpis",
    "facility_metadata": "indoor_kpis",
}


def resolve_table_name(table_name: str) -> str:
    """Resolve table aliases to actual table names."""
    return TABLE_ALIASES.get(table_name, table_name)


def validate_table(table_name: str) -> bool:
    """Validate that table is in allowlist."""
    resolved = resolve_table_name(table_name)
    return resolved in ALLOWED_TABLES


def validate_columns(table_name: str, columns: list) -> bool:
    """Validate that all columns are in allowlist for the table."""
    resolved = resolve_table_name(table_name)
    if resolved not in ALLOWED_TABLES:
        return False

    allowed = set(ALLOWED_TABLES[resolved])
    requested = set(columns)

    return requested.issubset(allowed)


def get_allowed_columns(table_name: str) -> list:
    """Get allowed columns for a table."""
    resolved = resolve_table_name(table_name)
    return ALLOWED_TABLES.get(resolved, [])


# Schema description for LLM SQL generation
SCHEMA_DESCRIPTION = {
    "sessions": {
        "description": "Usage sessions data: strokes, activity dates, facility/bay info",
        "common_uses": ["Usage patterns", "Stroke counts", "Activity analysis"],
        "key_columns": {
            "activity_date": "Date of activity (use for time filtering)",
            "facility": "Facility name",
            "sg_facility_id": "Facility ID for joins",
            "strokes_total": "Total strokes in session",
            "session_count": "Number of sessions",
        },
    },
    "connectivity_logs": {
        "description": "Network connectivity and disconnection events per facility/bay",
        "common_uses": ["Connectivity issues", "Disconnection analysis", "Network health", "Which facilities have problems"],
        "key_columns": {
            "log_date": "Date of event (use for time filtering with >= CURRENT_DATE - INTERVAL)",
            "facility_id": "Facility UUID",
            "facility_name": "Facility name (use for display and grouping)",
            "disconnection_cnt": "Count of disconnections (SUM for totals)",
            "connection_type": "Type of connection (wifi, ethernet, etc.)",
            "speed_mbps": "Connection speed in Mbps",
        },
    },
    "error_logs": {
        "description": "System errors and alerts from Trackman devices",
        "common_uses": ["Error tracking", "System alerts", "Finding problem facilities", "Most common errors"],
        "key_columns": {
            "error_timestamp": "Timestamp of error (use for time filtering with >= CURRENT_DATE - INTERVAL)",
            "facility_id": "Facility UUID",
            "facility_name": "Facility name (use for display and grouping)",
            "event_level": "Severity level (CRITICAL, ERROR, WARNING, INFO) - use UPPER() for comparison",
            "message": "The error message content (GROUP BY for common errors)",
            "message_id": "Unique message identifier",
        },
    },
    "indoor_kpis": {
        "description": "Key performance indicators per facility - use for facility health and metrics",
        "common_uses": ["Performance metrics", "Facility comparison", "KPI analysis", "Facility health"],
        "key_columns": {
            "facility_name": "Facility name",
            "facility_id": "Facility UUID",
            "occupancy": "Facility occupancy rate",
            "total_strokes": "Total strokes tracked",
            "strokes_per_bay": "Average strokes per bay",
            "active_units": "Number of active units",
            "configured_bays": "Number of configured bays",
        },
    },
}


def get_schema_for_prompt() -> str:
    """Generate schema description for LLM prompt."""
    lines = ["Available tables and columns:\n"]
    for table, columns in ALLOWED_TABLES.items():
        desc = SCHEMA_DESCRIPTION.get(table, {}).get("description", "")
        lines.append(f"### {table}")
        if desc:
            lines.append(f"Description: {desc}")
        lines.append(f"Columns: {', '.join(columns)}")
        key_cols = SCHEMA_DESCRIPTION.get(table, {}).get("key_columns", {})
        if key_cols:
            lines.append("Key columns:")
            for col, col_desc in key_cols.items():
                lines.append(f"  - {col}: {col_desc}")
        lines.append("")
    return "\n".join(lines)


# System prompt for SQL generation
SQL_GENERATION_SYSTEM_PROMPT = """You are a SQL query generator for a Trackman data warehouse.
Your task is to convert natural language questions into valid PostgreSQL/Redshift SQL queries.

{schema}

IMPORTANT TABLE NAMES (use exactly these):
- error_logs: Contains all error/alert data. Use error_timestamp for time filtering.
- connectivity_logs: Contains disconnection data. Use log_date for time filtering.
- sessions: Contains usage/stroke data. Use activity_date for time filtering.
- indoor_kpis: Contains facility KPI metrics.

RULES:
1. ONLY generate SELECT queries - never INSERT, UPDATE, DELETE, DROP, etc.
2. ONLY use tables and columns from the schema above.
3. Use appropriate date filtering when the user mentions time periods.
4. Current date is: {current_date}
5. Always include a LIMIT clause (max 100 rows) unless the user specifically asks for all data.
6. For time filtering:
   - error_logs: WHERE error_timestamp >= CURRENT_DATE - INTERVAL 'N days'
   - connectivity_logs: WHERE log_date >= CURRENT_DATE - INTERVAL 'N days'
   - sessions: WHERE activity_date >= CURRENT_DATE - INTERVAL 'N days'
7. Use UPPER() for case-insensitive event_level comparisons.
8. Return ONLY the SQL query, no explanations or markdown.
9. Always include facility_name in SELECT for readability.
10. For "most common" queries, use GROUP BY with COUNT(*) and ORDER BY DESC.
11. For "worst" or "most problems" queries, aggregate errors or disconnections and ORDER BY DESC.

Examples:
- "Show me errors from the last week" ->
SELECT facility_name, facility_id, message, event_level, error_timestamp FROM error_logs WHERE error_timestamp >= CURRENT_DATE - INTERVAL '7 days' ORDER BY error_timestamp DESC LIMIT 100

- "Top 5 facilities by disconnections" ->
SELECT facility_name, SUM(disconnection_cnt) as total_disconnections FROM connectivity_logs WHERE log_date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY facility_name ORDER BY total_disconnections DESC LIMIT 5

- "Most common error messages" ->
SELECT message, COUNT(*) as count FROM error_logs WHERE error_timestamp >= CURRENT_DATE - INTERVAL '7 days' GROUP BY message ORDER BY count DESC LIMIT 20

- "Which facilities have the most problems" ->
SELECT facility_name, COUNT(*) as error_count FROM error_logs WHERE error_timestamp >= CURRENT_DATE - INTERVAL '7 days' GROUP BY facility_name ORDER BY error_count DESC LIMIT 20
"""


# Dangerous SQL patterns to block
DANGEROUS_PATTERNS = [
    r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|GRANT|REVOKE)\b",
    r";",  # No multiple statements
    r"--",  # No SQL comments
    r"/\*",  # No block comments
    r"\bEXEC\b",
    r"\bEXECUTE\b",
    r"\bINTO\s+OUTFILE\b",
    r"\bINTO\s+DUMPFILE\b",
]


def validate_generated_sql(sql_query: str) -> tuple:
    """
    Validate that generated SQL is safe to execute.

    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    import re

    sql_upper = sql_query.upper().strip()

    # Must be a SELECT query
    if not sql_upper.startswith("SELECT"):
        return False, "Only SELECT queries are allowed"

    # Check for dangerous patterns
    for pattern in DANGEROUS_PATTERNS:
        if re.search(pattern, sql_query, re.IGNORECASE):
            return False, "Query contains disallowed pattern"

    # Validate tables used
    all_tables = set(ALLOWED_TABLES.keys()) | set(TABLE_ALIASES.keys())
    from_match = re.search(r"\bFROM\s+(\w+)", sql_query, re.IGNORECASE)
    join_matches = re.findall(r"\bJOIN\s+(\w+)", sql_query, re.IGNORECASE)

    tables_used = []
    if from_match:
        tables_used.append(from_match.group(1).lower())
    tables_used.extend([t.lower() for t in join_matches])

    for table in tables_used:
        if table not in all_tables:
            return False, f"Table '{table}' is not in the allowed list"

    return True, None


def add_limit_if_missing(sql_query: str, max_rows: int = 100) -> str:
    """Add LIMIT clause if not present."""
    import re
    if not re.search(r"\bLIMIT\s+\d+", sql_query, re.IGNORECASE):
        return f"{sql_query.rstrip()} LIMIT {max_rows}"
    return sql_query
