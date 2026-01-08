"""Interface for Trackman data sources."""

from abc import ABC, abstractmethod
from typing import Dict, Optional


class TrackmanDataSource(ABC):
    """Abstract base class for Trackman data sources."""

    @abstractmethod
    def get_errors_summary(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """
        Get summary of errors within the specified time range.

        Args:
            range_days: Number of days to look back
            facility_id: Optional facility ID to filter by

        Returns:
            Dict with structure:
            {
                "columns": ["facility_id", "error_count", "critical_count", ...],
                "rows": [[...], ...],
                "metadata": {"source": "...", "range_days": N, "rowCount": X}
            }
        """
        pass

    @abstractmethod
    def get_top_error_messages(
        self, range_days: int, limit: int = 10, facility_id: Optional[str] = None
    ) -> Dict:
        """
        Get top error messages by frequency.

        Args:
            range_days: Number of days to look back
            limit: Maximum number of results to return
            facility_id: Optional facility ID to filter by

        Returns:
            Dict with structure:
            {
                "columns": ["error_message", "count", "severity", ...],
                "rows": [[...], ...],
                "metadata": {"source": "...", "range_days": N, "rowCount": X}
            }
        """
        pass

    @abstractmethod
    def get_connectivity_summary(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """
        Get connectivity status summary.

        Args:
            range_days: Number of days to look back
            facility_id: Optional facility ID to filter by

        Returns:
            Dict with structure:
            {
                "columns": ["facility_id", "total_events", "connected_pct", ...],
                "rows": [[...], ...],
                "metadata": {"source": "...", "range_days": N, "rowCount": X}
            }
        """
        pass

    @abstractmethod
    def get_disconnect_reasons(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """
        Get disconnect reasons breakdown.

        Args:
            range_days: Number of days to look back
            facility_id: Optional facility ID to filter by

        Returns:
            Dict with structure:
            {
                "columns": ["disconnect_reason", "count", "percentage"],
                "rows": [[...], ...],
                "metadata": {"source": "...", "range_days": N, "rowCount": X}
            }
        """
        pass

    @abstractmethod
    def get_facility_summary(self, facility_id: str, range_days: int) -> Dict:
        """
        Get comprehensive summary for a specific facility.

        Args:
            facility_id: Facility ID
            range_days: Number of days to look back for metrics

        Returns:
            Dict with structure:
            {
                "columns": ["metric", "value"],
                "rows": [[...], ...],
                "metadata": {"source": "...", "facility_id": "...", "range_days": N}
            }
        """
        pass

    @abstractmethod
    def get_data_quality_summary(
        self, range_days: int, facility_id: Optional[str] = None
    ) -> Dict:
        """
        Get data quality metrics summary.

        Args:
            range_days: Number of days to look back
            facility_id: Optional facility ID to filter by

        Returns:
            Dict with structure:
            {
                "columns": ["facility_id", "avg_quality_score", "missing_records", ...],
                "rows": [[...], ...],
                "metadata": {"source": "...", "range_days": N, "rowCount": X}
            }
        """
        pass
