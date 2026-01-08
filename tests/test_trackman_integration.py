"""
Tests for Trackman data integration.
Can run against Excel source or mock Redshift connection.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

# Add code directory to path
sys.path.append(str(Path(__file__).parent.parent / "code"))

from backend.batch.utilities.helpers.trackman.data_source_factory import get_data_source
from backend.batch.utilities.helpers.trackman.excel_data_source import ExcelDataSource


class TestExcelDataSource:
    """Test Excel data source implementation."""

    @pytest.fixture
    def data_dir(self, tmp_path):
        """Create temporary directory with test Excel file."""
        import pandas as pd

        test_dir = tmp_path / "testdata"
        test_dir.mkdir()

        # Create sample data
        errors_data = pd.DataFrame({
            "timestamp": pd.date_range(start="2026-01-01", periods=10, freq="D"),
            "facility_id": ["FAC001"] * 5 + ["FAC002"] * 5,
            "unit_id": ["U001", "U002"] * 5,
            "unit_model": ["TrackMan 4"] * 10,
            "error_code": ["E001", "E002", "E003"] * 3 + ["E001"],
            "severity": ["LOW", "MEDIUM", "HIGH"] * 3 + ["CRITICAL"],
            "error_message": ["Test error"] * 10,
        })

        connectivity_data = pd.DataFrame({
            "timestamp": pd.date_range(start="2026-01-01", periods=10, freq="D"),
            "facility_id": ["FAC001"] * 10,
            "unit_id": ["U001"] * 10,
            "connectivity_status": ["ONLINE"] * 8 + ["OFFLINE"] * 2,
            "disconnect_reason": [None] * 8 + ["Network timeout", "Power loss"],
        })

        facility_data = pd.DataFrame({
            "facility_id": ["FAC001", "FAC002"],
            "location": ["New York", "Los Angeles"],
            "opening_hours": ["9am-9pm", "8am-10pm"],
            "subscription_status": ["ACTIVE", "ACTIVE"],
            "units_deployed": [5, 3],
            "usage_hours_30d": [245.5, 180.0],
            "strokes_tracked": [125000, 89000],
            "tournaments_hosted": [8, 5],
        })

        quality_data = pd.DataFrame({
            "timestamp": pd.date_range(start="2026-01-01", periods=10, freq="D"),
            "facility_id": ["FAC001"] * 10,
            "data_quality_score": [85.0, 90.0, 88.0, 92.0, 87.0] * 2,
            "missing_records": [5, 3, 4, 2, 6] * 2,
            "latency_ms": [45.0, 38.0, 42.0, 35.0, 50.0] * 2,
        })

        # Write to Excel file
        excel_path = test_dir / "test_data.xlsx"
        with pd.ExcelWriter(excel_path) as writer:
            errors_data.to_excel(writer, sheet_name="errors", index=False)
            connectivity_data.to_excel(writer, sheet_name="connectivity", index=False)
            facility_data.to_excel(writer, sheet_name="facility_metadata", index=False)
            quality_data.to_excel(writer, sheet_name="data_quality", index=False)

        return test_dir

    def test_excel_source_initialization(self, data_dir):
        """Test Excel source loads data correctly."""
        source = ExcelDataSource(data_dir=str(data_dir))

        assert len(source._data["errors"]) == 10
        assert len(source._data["connectivity"]) == 10
        assert len(source._data["facility_metadata"]) == 2
        assert len(source._data["data_quality"]) == 10

    def test_get_errors_summary(self, data_dir):
        """Test errors summary query."""
        source = ExcelDataSource(data_dir=str(data_dir))

        result = source.get_errors_summary(range_days=30)

        assert "columns" in result
        assert "rows" in result
        assert "metadata" in result
        assert result["metadata"]["source"] == "excel"
        assert len(result["rows"]) > 0

    def test_get_errors_summary_with_facility_filter(self, data_dir):
        """Test errors summary with facility filter."""
        source = ExcelDataSource(data_dir=str(data_dir))

        result = source.get_errors_summary(range_days=30, facility_id="FAC001")

        assert len(result["rows"]) == 5  # Only FAC001 errors

        # Verify all rows are for FAC001
        facility_col_idx = result["columns"].index("facility_id")
        for row in result["rows"]:
            assert row[facility_col_idx] == "FAC001"

    def test_get_top_error_messages(self, data_dir):
        """Test top error messages query."""
        source = ExcelDataSource(data_dir=str(data_dir))

        result = source.get_top_error_messages(range_days=30, limit=5)

        assert len(result["rows"]) <= 5
        assert "error_code" in result["columns"]
        assert "count" in result["columns"]

    def test_get_connectivity_summary(self, data_dir):
        """Test connectivity summary."""
        source = ExcelDataSource(data_dir=str(data_dir))

        result = source.get_connectivity_summary(range_days=30)

        assert "connectivity_status" in result["columns"]
        assert len(result["rows"]) > 0

    def test_get_facility_summary(self, data_dir):
        """Test facility summary."""
        source = ExcelDataSource(data_dir=str(data_dir))

        result = source.get_facility_summary(facility_id="FAC001", range_days=30)

        assert "metadata" in result
        assert len(result["rows"]) > 0

    def test_multiple_excel_files(self, tmp_path):
        """Test merging data from multiple Excel files."""
        import pandas as pd

        test_dir = tmp_path / "multifile"
        test_dir.mkdir()

        # Create two files with errors
        errors1 = pd.DataFrame({
            "timestamp": pd.date_range(start="2026-01-01", periods=5, freq="D"),
            "facility_id": ["FAC001"] * 5,
            "unit_id": ["U001"] * 5,
            "unit_model": ["TrackMan 4"] * 5,
            "error_code": ["E001"] * 5,
            "severity": ["LOW"] * 5,
            "error_message": ["Error 1"] * 5,
        })

        errors2 = pd.DataFrame({
            "timestamp": pd.date_range(start="2026-01-06", periods=5, freq="D"),
            "facility_id": ["FAC002"] * 5,
            "unit_id": ["U002"] * 5,
            "unit_model": ["TrackMan iO"] * 5,
            "error_code": ["E002"] * 5,
            "severity": ["MEDIUM"] * 5,
            "error_message": ["Error 2"] * 5,
        })

        # Write to separate files
        with pd.ExcelWriter(test_dir / "file1.xlsx") as writer:
            errors1.to_excel(writer, sheet_name="errors", index=False)

        with pd.ExcelWriter(test_dir / "file2.xlsx") as writer:
            errors2.to_excel(writer, sheet_name="errors", index=False)

        # Load and verify merge
        source = ExcelDataSource(data_dir=str(test_dir))

        assert len(source._data["errors"]) == 10  # Should have merged both


class TestDataSourceFactory:
    """Test data source factory."""

    def test_factory_returns_excel_by_default(self):
        """Test factory returns Excel source when Redshift not configured."""
        with patch.dict(os.environ, {"USE_REDSHIFT": "false"}, clear=False):
            source = get_data_source()
            assert isinstance(source, ExcelDataSource)

    def test_factory_env_variable_parsing(self):
        """Test factory correctly parses environment variables."""
        # Missing Redshift config should fall back to Excel
        with patch.dict(os.environ, {"USE_REDSHIFT": "true", "REDSHIFT_HOST": ""}, clear=False):
            source = get_data_source()
            assert isinstance(source, ExcelDataSource)


class TestRedshiftDataSource:
    """Test Redshift data source with mocked connection."""

    @pytest.fixture
    def mock_redshift_connection(self):
        """Mock Redshift connection."""
        with patch("backend.batch.utilities.helpers.trackman.redshift_data_source.psycopg2") as mock_pg:
            mock_conn = Mock()
            mock_cursor = Mock()

            mock_pg.connect.return_value = mock_conn
            mock_conn.cursor.return_value = mock_cursor

            # Mock query results
            mock_cursor.fetchall.return_value = [
                ("FAC001", 10, "HIGH"),
                ("FAC002", 5, "MEDIUM"),
            ]
            mock_cursor.description = [
                Mock(name="facility_id"),
                Mock(name="error_count"),
                Mock(name="severity"),
            ]

            yield mock_pg, mock_cursor

    def test_redshift_connection_uses_parameterized_queries(self, mock_redshift_connection):
        """Verify Redshift source uses parameterized queries."""
        from backend.batch.utilities.helpers.trackman.redshift_data_source import RedshiftDataSource

        mock_pg, mock_cursor = mock_redshift_connection

        with patch.dict(os.environ, {
            "REDSHIFT_HOST": "test-host",
            "REDSHIFT_DB": "testdb",
            "REDSHIFT_USER": "testuser",
            "REDSHIFT_PASSWORD": "testpass",
        }):
            source = RedshiftDataSource()
            result = source.get_errors_summary(range_days=7, facility_id="FAC001")

            # Verify execute was called with parameterized query
            assert mock_cursor.execute.called
            call_args = mock_cursor.execute.call_args

            # First argument should be SQL with %s placeholders
            assert "%s" in call_args[0][0] or "?" in call_args[0][0]
            # Second argument should be tuple of parameters
            assert isinstance(call_args[0][1], (tuple, list))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
