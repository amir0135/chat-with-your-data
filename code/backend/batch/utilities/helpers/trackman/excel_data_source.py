"""Excel-based Trackman data source implementation.
Scans data/testtrack directory for Excel files and merges data from all sheets."""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .data_source_interface import TrackmanDataSource

logger = logging.getLogger(__name__)


class ExcelDataSource(TrackmanDataSource):
    """Excel-based implementation of Trackman data source.

    Scans a directory for all .xlsx and .xls files and merges data
    from matching sheets across all files.
    """

    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize Excel data source.

        Args:
            data_dir: Directory containing Excel files. If None, uses TRACKMAN_DATA_DIR env var
                     or defaults to 'data/testtrack'. Will scan for all .xlsx files
                     and merge data from matching sheets.
        """
        if data_dir is None:
            data_dir = os.getenv("TRACKMAN_DATA_DIR", "data/testtrack")

        self.data_dir = Path(data_dir)
        self._data = {}
        self._load_data()

    def _load_data(self):
        """Load all sheets from all Excel files in directory and merge them."""
        try:
            logger.info(f"Scanning for Trackman Excel files in: {self.data_dir}")

            if not self.data_dir.exists():
                logger.warning(f"Data directory not found: {self.data_dir}")
                self._initialize_empty_data()
                return

            # Find all Excel files
            excel_files = list(self.data_dir.glob("*.xlsx")) + list(self.data_dir.glob("*.xls"))

            if not excel_files:
                logger.warning(f"No Excel files found in {self.data_dir}")
                self._initialize_empty_data()
                return

            logger.info(f"Found {len(excel_files)} Excel file(s): {[f.name for f in excel_files]}")

            # Initialize accumulators for each sheet type
            expected_sheets = ["errors", "connectivity", "facility_metadata", "data_quality"]
            sheet_dataframes = {sheet: [] for sheet in expected_sheets}

            # Load data from each file
            for excel_path in excel_files:
                try:
                    logger.info(f"Loading file: {excel_path.name}")
                    excel_file = pd.ExcelFile(excel_path)

                    for sheet_name in expected_sheets:
                        if sheet_name in excel_file.sheet_names:
                            df = pd.read_excel(excel_file, sheet_name=sheet_name)

                            # Convert timestamp columns to datetime
                            if "timestamp" in df.columns:
                                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

                            sheet_dataframes[sheet_name].append(df)
                            logger.info(f"  Loaded sheet '{sheet_name}' with {len(df)} rows")
                        else:
                            logger.debug(f"  Sheet '{sheet_name}' not found in {excel_path.name}")

                except Exception as e:
                    logger.error(f"Error loading file {excel_path.name}: {e}")
                    continue

            # Merge all dataframes for each sheet
            for sheet_name in expected_sheets:
                dfs = sheet_dataframes[sheet_name]
                if dfs:
                    merged_df = pd.concat(dfs, ignore_index=True)
                    # Remove duplicate rows
                    merged_df = merged_df.drop_duplicates()
                    self._data[sheet_name] = merged_df
                    logger.info(f"Merged sheet '{sheet_name}': {len(merged_df)} total rows")
                else:
                    logger.warning(f"No data found for sheet '{sheet_name}' across all files")
                    self._data[sheet_name] = pd.DataFrame()

            logger.info("Excel data loaded and merged successfully")

        except Exception as e:
            logger.error(f"Error loading Excel files: {str(e)}")
            self._initialize_empty_data()

    def _initialize_empty_data(self):
        """Initialize empty dataframes for all expected sheets."""
        for sheet in ["errors", "connectivity", "facility_metadata", "data_quality"]:
            self._data[sheet] = pd.DataFrame()

    def _filter_by_date_range(self, df: pd.DataFrame, range_days: int) -> pd.DataFrame:
        """Filter dataframe by timestamp within range_days."""
        if "timestamp" not in df.columns or df.empty:
            return df

        cutoff_date = datetime.now() - timedelta(days=range_days)
        return df[df["timestamp"] >= cutoff_date]

    def _filter_by_facility(
        self, df: pd.DataFrame, facility_id: Optional[str]
    ) -> pd.DataFrame:
        """Filter dataframe by facility_id if provided."""
        if facility_id and "facility_id" in df.columns:
            return df[df["facility_id"] == facility_id]
        return df

    def _format_result(
        self, df: pd.DataFrame, metadata: Dict, source: str = "excel"
    ) -> Dict:
        """Format dataframe as result dict."""
        if df.empty:
            return {
                "columns": [],
                "rows": [],
                "metadata": {**metadata, "source": source, "rowCount": 0},
            }

        # Convert DataFrame to list of lists, handling NaN values
        rows = df.fillna("").values.tolist()

        return {
            "columns": df.columns.tolist(),
            "rows": rows,
            "metadata": {**metadata, "source": source, "rowCount": len(df)},
        }

    def get_errors_summary(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """Get summary of errors within the specified time range."""
        try:
            df = self._data.get("errors", pd.DataFrame())
            df = self._filter_by_date_range(df, range_days)
            df = self._filter_by_facility(df, facility_id)

            if df.empty:
                return self._format_result(
                    df, {"range_days": range_days, "facility_id": facility_id}
                )

            # Group by facility and summarize
            summary = (
                df.groupby("facility_id")
                .agg(
                    {
                        "error_code": "count",
                        "severity": lambda x: (x == "critical").sum(),
                    }
                )
                .reset_index()
            )
            summary.columns = ["facility_id", "error_count", "critical_count"]

            # Add unique error types
            unique_errors = (
                df.groupby("facility_id")["error_code"].nunique().reset_index()
            )
            unique_errors.columns = ["facility_id", "unique_errors"]
            summary = summary.merge(unique_errors, on="facility_id")

            return self._format_result(
                summary, {"range_days": range_days, "facility_id": facility_id}
            )
        except Exception as e:
            logger.error(f"Error in get_errors_summary: {str(e)}")
            raise

    def get_top_error_messages(
        self, range_days: int, limit: int = 10, facility_id: Optional[str] = None
    ) -> Dict:
        """Get top error messages by frequency."""
        try:
            df = self._data.get("errors", pd.DataFrame())
            df = self._filter_by_date_range(df, range_days)
            df = self._filter_by_facility(df, facility_id)

            if df.empty:
                return self._format_result(
                    df,
                    {
                        "range_days": range_days,
                        "limit": limit,
                        "facility_id": facility_id,
                    },
                )

            # Count error messages and get most severe severity for each
            error_summary = (
                df.groupby(["error_message", "error_code"])
                .agg({"error_code": "size", "severity": "first"})
                .reset_index()
            )
            error_summary.columns = [
                "error_message",
                "error_code",
                "count",
                "severity",
            ]

            # Sort by count and limit
            error_summary = error_summary.sort_values("count", ascending=False).head(
                limit
            )

            return self._format_result(
                error_summary,
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
            df = self._data.get("connectivity", pd.DataFrame())
            df = self._filter_by_date_range(df, range_days)
            df = self._filter_by_facility(df, facility_id)

            if df.empty:
                return self._format_result(
                    df, {"range_days": range_days, "facility_id": facility_id}
                )

            # Calculate connectivity metrics per facility
            summary = df.groupby("facility_id").agg(
                total_events=("connectivity_status", "size"),
                connected_count=(
                    "connectivity_status",
                    lambda x: (x == "connected").sum(),
                ),
            )

            summary["connected_pct"] = (
                summary["connected_count"] / summary["total_events"] * 100
            ).round(2)
            summary = summary.reset_index()

            return self._format_result(
                summary, {"range_days": range_days, "facility_id": facility_id}
            )
        except Exception as e:
            logger.error(f"Error in get_connectivity_summary: {str(e)}")
            raise

    def get_disconnect_reasons(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """Get disconnect reasons breakdown."""
        try:
            df = self._data.get("connectivity", pd.DataFrame())
            df = self._filter_by_date_range(df, range_days)
            df = self._filter_by_facility(df, facility_id)

            if df.empty:
                return self._format_result(
                    df, {"range_days": range_days, "facility_id": facility_id}
                )

            # Filter for disconnected events only
            disconnected = df[df["connectivity_status"] == "disconnected"]

            if disconnected.empty:
                return self._format_result(
                    disconnected, {"range_days": range_days, "facility_id": facility_id}
                )

            # Count disconnect reasons
            reasons = disconnected["disconnect_reason"].value_counts().reset_index()
            reasons.columns = ["disconnect_reason", "count"]

            # Calculate percentage
            total = reasons["count"].sum()
            reasons["percentage"] = (reasons["count"] / total * 100).round(2)

            return self._format_result(
                reasons, {"range_days": range_days, "facility_id": facility_id}
            )
        except Exception as e:
            logger.error(f"Error in get_disconnect_reasons: {str(e)}")
            raise

    def get_facility_summary(self, facility_id: str, range_days: int) -> Dict:
        """Get comprehensive summary for a specific facility."""
        try:
            # Get facility metadata
            metadata_df = self._data.get("facility_metadata", pd.DataFrame())
            facility_meta = metadata_df[metadata_df["facility_id"] == facility_id]

            if facility_meta.empty:
                logger.warning(f"No metadata found for facility {facility_id}")
                return self._format_result(
                    pd.DataFrame(),
                    {"facility_id": facility_id, "range_days": range_days},
                )

            # Build summary metrics
            metrics = []

            # Add metadata
            for col in facility_meta.columns:
                if col != "facility_id":
                    metrics.append([col, str(facility_meta[col].iloc[0])])

            # Add error metrics
            errors_df = self._data.get("errors", pd.DataFrame())
            errors_filtered = self._filter_by_date_range(errors_df, range_days)
            errors_filtered = errors_filtered[
                errors_filtered["facility_id"] == facility_id
            ]

            if not errors_filtered.empty:
                metrics.append(["errors_total", len(errors_filtered)])
                metrics.append(
                    [
                        "errors_critical",
                        (errors_filtered["severity"] == "critical").sum(),
                    ]
                )

            # Add connectivity metrics
            conn_df = self._data.get("connectivity", pd.DataFrame())
            conn_filtered = self._filter_by_date_range(conn_df, range_days)
            conn_filtered = conn_filtered[conn_filtered["facility_id"] == facility_id]

            if not conn_filtered.empty:
                connected_pct = (
                    (conn_filtered["connectivity_status"] == "connected").sum()
                    / len(conn_filtered)
                    * 100
                )
                metrics.append(["connectivity_pct", round(connected_pct, 2)])

            # Add data quality metrics
            quality_df = self._data.get("data_quality", pd.DataFrame())
            quality_filtered = self._filter_by_date_range(quality_df, range_days)
            quality_filtered = quality_filtered[
                quality_filtered["facility_id"] == facility_id
            ]

            if not quality_filtered.empty:
                metrics.append(
                    [
                        "avg_data_quality_score",
                        round(quality_filtered["data_quality_score"].mean(), 2),
                    ]
                )

            result_df = pd.DataFrame(metrics, columns=["metric", "value"])

            return self._format_result(
                result_df, {"facility_id": facility_id, "range_days": range_days}
            )
        except Exception as e:
            logger.error(f"Error in get_facility_summary: {str(e)}")
            raise

    def get_data_quality_summary(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """Get data quality metrics summary."""
        try:
            df = self._data.get("data_quality", pd.DataFrame())
            df = self._filter_by_date_range(df, range_days)
            df = self._filter_by_facility(df, facility_id)

            if df.empty:
                return self._format_result(
                    df, {"range_days": range_days, "facility_id": facility_id}
                )

            # Calculate quality metrics per facility
            summary = (
                df.groupby("facility_id")
                .agg(
                    {
                        "data_quality_score": "mean",
                        "missing_records": "sum",
                        "latency_ms": "mean",
                    }
                )
                .reset_index()
            )

            summary.columns = [
                "facility_id",
                "avg_quality_score",
                "total_missing_records",
                "avg_latency_ms",
            ]

            # Round decimals
            summary["avg_quality_score"] = summary["avg_quality_score"].round(2)
            summary["avg_latency_ms"] = summary["avg_latency_ms"].round(2)

            return self._format_result(
                summary, {"range_days": range_days, "facility_id": facility_id}
            )
        except Exception as e:
            logger.error(f"Error in get_data_quality_summary: {str(e)}")
            raise
