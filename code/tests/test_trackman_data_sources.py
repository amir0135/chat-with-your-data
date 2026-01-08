"""Tests for Trackman data sources."""

import os
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from code.backend.batch.utilities.helpers.trackman.excel_data_source import (
    ExcelDataSource,
)
from code.backend.batch.utilities.helpers.trackman.redshift_config import (
    validate_columns,
    validate_table,
)
from code.backend.batch.utilities.helpers.trackman.redshift_data_source import (
    RedshiftDataSource,
)
from code.backend.batch.utilities.helpers.trackman.data_source_factory import (
    get_data_source,
    reset_data_source,
)


@pytest.fixture
def sample_excel_data(tmp_path):
    """Create sample Excel file for testing."""
    excel_path = tmp_path / "test_trackman_data.xlsx"

    # Create sample data
    now = datetime.now()

    errors_data = pd.DataFrame(
        {
            "timestamp": [now - timedelta(days=i) for i in range(10)],
            "facility_id": ["FAC001"] * 5 + ["FAC002"] * 5,
            "unit_id": ["U001", "U002"] * 5,
            "unit_model": ["ModelA"] * 10,
            "error_code": ["E001", "E002", "E003"] * 3 + ["E001"],
            "severity": ["critical", "warning"] * 5,
            "error_message": ["Error message 1", "Error message 2"] * 5,
        }
    )

    connectivity_data = pd.DataFrame(
        {
            "timestamp": [now - timedelta(days=i) for i in range(10)],
            "facility_id": ["FAC001"] * 5 + ["FAC002"] * 5,
            "unit_id": ["U001", "U002"] * 5,
            "connectivity_status": ["connected", "disconnected"] * 5,
            "disconnect_reason": [
                "",
                "network_issue",
                "",
                "power_loss",
                "",
                "timeout",
            ]
            * 2
            + ["", "network_issue"],
        }
    )

    facility_metadata = pd.DataFrame(
        {
            "facility_id": ["FAC001", "FAC002"],
            "location": ["New York", "Los Angeles"],
            "opening_hours": ["9-5", "8-6"],
            "subscription_status": ["active", "active"],
            "units_deployed": [10, 15],
            "usage_hours_30d": [240, 360],
            "strokes_tracked": [50000, 75000],
            "tournaments_hosted": [5, 8],
        }
    )

    data_quality = pd.DataFrame(
        {
            "timestamp": [now - timedelta(days=i) for i in range(10)],
            "facility_id": ["FAC001"] * 5 + ["FAC002"] * 5,
            "data_quality_score": [95.5, 94.0, 96.0, 93.5, 95.0, 97.0, 96.5, 95.5, 94.5, 96.0],
            "missing_records": [5, 10, 3, 8, 6, 2, 4, 7, 9, 5],
            "latency_ms": [120, 150, 110, 140, 130, 100, 115, 125, 135, 120],
        }
    )

    # Write to Excel
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        errors_data.to_excel(writer, sheet_name="errors", index=False)
        connectivity_data.to_excel(writer, sheet_name="connectivity", index=False)
        facility_metadata.to_excel(writer, sheet_name="facility_metadata", index=False)
        data_quality.to_excel(writer, sheet_name="data_quality", index=False)

    return str(excel_path)


class TestExcelDataSource:
    """Tests for Excel data source."""

    def test_load_data(self, sample_excel_data):
        """Test loading data from Excel."""
        ds = ExcelDataSource(sample_excel_data)
        assert "errors" in ds._data
        assert "connectivity" in ds._data
        assert "facility_metadata" in ds._data
        assert "data_quality" in ds._data

    def test_get_errors_summary(self, sample_excel_data):
        """Test errors summary query."""
        ds = ExcelDataSource(sample_excel_data)
        result = ds.get_errors_summary(range_days=30)

        assert result["metadata"]["source"] == "excel"
        assert result["metadata"]["rowCount"] > 0
        assert "facility_id" in result["columns"]
        assert "error_count" in result["columns"]
        assert "critical_count" in result["columns"]

    def test_get_errors_summary_with_facility_filter(self, sample_excel_data):
        """Test errors summary with facility filter."""
        ds = ExcelDataSource(sample_excel_data)
        result = ds.get_errors_summary(range_days=30, facility_id="FAC001")

        assert result["metadata"]["rowCount"] >= 0
        if result["rows"]:
            assert all(row[0] == "FAC001" for row in result["rows"])

    def test_get_top_error_messages(self, sample_excel_data):
        """Test top error messages query."""
        ds = ExcelDataSource(sample_excel_data)
        result = ds.get_top_error_messages(range_days=30, limit=5)

        assert result["metadata"]["source"] == "excel"
        assert len(result["rows"]) <= 5
        assert "error_message" in result["columns"]
        assert "count" in result["columns"]

    def test_get_connectivity_summary(self, sample_excel_data):
        """Test connectivity summary query."""
        ds = ExcelDataSource(sample_excel_data)
        result = ds.get_connectivity_summary(range_days=30)

        assert result["metadata"]["source"] == "excel"
        assert "facility_id" in result["columns"]
        assert "connected_pct" in result["columns"]

    def test_get_disconnect_reasons(self, sample_excel_data):
        """Test disconnect reasons query."""
        ds = ExcelDataSource(sample_excel_data)
        result = ds.get_disconnect_reasons(range_days=30)

        assert result["metadata"]["source"] == "excel"
        assert "disconnect_reason" in result["columns"]
        assert "percentage" in result["columns"]

    def test_get_facility_summary(self, sample_excel_data):
        """Test facility summary query."""
        ds = ExcelDataSource(sample_excel_data)
        result = ds.get_facility_summary("FAC001", range_days=30)

        assert result["metadata"]["source"] == "excel"
        assert result["metadata"]["facility_id"] == "FAC001"
        assert "metric" in result["columns"]
        assert "value" in result["columns"]

    def test_get_data_quality_summary(self, sample_excel_data):
        """Test data quality summary query."""
        ds = ExcelDataSource(sample_excel_data)
        result = ds.get_data_quality_summary(range_days=30)

        assert result["metadata"]["source"] == "excel"
        assert "facility_id" in result["columns"]
        assert "avg_quality_score" in result["columns"]


class TestRedshiftConfig:
    """Tests for Redshift configuration and allowlists."""

    def test_validate_table_allowed(self):
        """Test validating allowed table."""
        assert validate_table("errors") is True
        assert validate_table("connectivity") is True
        assert validate_table("facility_metadata") is True
        assert validate_table("data_quality") is True

    def test_validate_table_not_allowed(self):
        """Test validating disallowed table."""
        assert validate_table("users") is False
        assert validate_table("sensitive_data") is False

    def test_validate_columns_allowed(self):
        """Test validating allowed columns."""
        assert validate_columns("errors", ["timestamp", "facility_id", "error_code"]) is True

    def test_validate_columns_not_allowed(self):
        """Test validating disallowed columns."""
        assert (
            validate_columns("errors", ["timestamp", "secret_column"]) is False
        )


class TestRedshiftDataSource:
    """Tests for Redshift data source."""

    @patch("psycopg2.connect")
    def test_initialization_success(self, mock_connect):
        """Test successful initialization with all env vars."""
        with patch.dict(
            os.environ,
            {
                "REDSHIFT_HOST": "test.redshift.amazonaws.com",
                "REDSHIFT_DB": "testdb",
                "REDSHIFT_USER": "testuser",
                "REDSHIFT_PASSWORD": "testpass",
            },
        ):
            ds = RedshiftDataSource()
            assert ds.host == "test.redshift.amazonaws.com"
            assert ds.database == "testdb"

    def test_initialization_failure(self):
        """Test initialization failure with missing env vars."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Missing required Redshift"):
                RedshiftDataSource()

    @patch("psycopg2.connect")
    def test_parameterized_query(self, mock_connect):
        """Test that queries use parameterized SQL."""
        # Setup mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("FAC001", 10, 2, 3)]
        mock_cursor.description = [
            ("facility_id",),
            ("error_count",),
            ("critical_count",),
            ("unique_errors",),
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        with patch.dict(
            os.environ,
            {
                "REDSHIFT_HOST": "test.redshift.amazonaws.com",
                "REDSHIFT_DB": "testdb",
                "REDSHIFT_USER": "testuser",
                "REDSHIFT_PASSWORD": "testpass",
            },
        ):
            ds = RedshiftDataSource()
            result = ds.get_errors_summary(range_days=30)

            # Verify parameterized query was called
            assert mock_cursor.execute.called
            call_args = mock_cursor.execute.call_args
            # Should have parameters tuple
            assert call_args[0][1] is not None or len(call_args[0]) > 1

    @patch("psycopg2.connect")
    def test_table_allowlist_enforcement(self, mock_connect):
        """Test that only allowlisted tables can be accessed."""
        with patch.dict(
            os.environ,
            {
                "REDSHIFT_HOST": "test.redshift.amazonaws.com",
                "REDSHIFT_DB": "testdb",
                "REDSHIFT_USER": "testuser",
                "REDSHIFT_PASSWORD": "testpass",
            },
        ):
            ds = RedshiftDataSource()

            # Should succeed for allowed table
            assert ds._validate_table_access("errors") is None

            # Should raise for disallowed table
            with pytest.raises(ValueError, match="not in the allowlist"):
                ds._validate_table_access("unauthorized_table")


class TestDataSourceFactory:
    """Tests for data source factory."""

    def teardown_method(self):
        """Reset data source after each test."""
        reset_data_source()

    def test_get_excel_data_source_default(self, sample_excel_data):
        """Test getting Excel data source by default."""
        with patch.dict(os.environ, {"TRACKMAN_EXCEL_PATH": sample_excel_data}):
            ds = get_data_source()
            assert isinstance(ds, ExcelDataSource)

    def test_get_redshift_data_source_with_config(self):
        """Test getting Redshift data source with proper config."""
        with patch.dict(
            os.environ,
            {
                "USE_REDSHIFT": "true",
                "REDSHIFT_HOST": "test.redshift.amazonaws.com",
                "REDSHIFT_DB": "testdb",
                "REDSHIFT_USER": "testuser",
                "REDSHIFT_PASSWORD": "testpass",
            },
        ):
            ds = get_data_source()
            assert isinstance(ds, RedshiftDataSource)

    def test_fallback_to_excel_on_missing_vars(self, sample_excel_data):
        """Test fallback to Excel when Redshift vars missing."""
        with patch.dict(
            os.environ,
            {"USE_REDSHIFT": "true", "TRACKMAN_EXCEL_PATH": sample_excel_data},
            clear=True,
        ):
            ds = get_data_source()
            assert isinstance(ds, ExcelDataSource)

    def test_singleton_behavior(self, sample_excel_data):
        """Test that factory returns same instance."""
        with patch.dict(os.environ, {"TRACKMAN_EXCEL_PATH": sample_excel_data}):
            ds1 = get_data_source()
            ds2 = get_data_source()
            assert ds1 is ds2
