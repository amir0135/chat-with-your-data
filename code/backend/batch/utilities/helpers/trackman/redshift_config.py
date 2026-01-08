"""Configuration for Redshift table and column allowlists."""

# Allowlist configuration for POC
# Only these tables and columns can be queried

ALLOWED_TABLES = {
    "errors": [
        "timestamp",
        "facility_id",
        "unit_id",
        "unit_model",
        "error_code",
        "severity",
        "error_message",
    ],
    "connectivity": [
        "timestamp",
        "facility_id",
        "unit_id",
        "connectivity_status",
        "disconnect_reason",
    ],
    "facility_metadata": [
        "facility_id",
        "location",
        "opening_hours",
        "subscription_status",
        "units_deployed",
        "usage_hours_30d",
        "strokes_tracked",
        "tournaments_hosted",
    ],
    "data_quality": [
        "timestamp",
        "facility_id",
        "data_quality_score",
        "missing_records",
        "latency_ms",
    ],
}


def validate_table(table_name: str) -> bool:
    """Validate that table is in allowlist."""
    return table_name in ALLOWED_TABLES


def validate_columns(table_name: str, columns: list) -> bool:
    """Validate that all columns are in allowlist for the table."""
    if table_name not in ALLOWED_TABLES:
        return False

    allowed = set(ALLOWED_TABLES[table_name])
    requested = set(columns)

    return requested.issubset(allowed)


def get_allowed_columns(table_name: str) -> list:
    """Get allowed columns for a table."""
    return ALLOWED_TABLES.get(table_name, [])
