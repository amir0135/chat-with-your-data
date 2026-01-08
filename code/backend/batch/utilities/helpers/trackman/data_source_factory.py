"""Factory for creating Trackman data sources."""

import logging
import os
from typing import Optional

from .data_source_interface import TrackmanDataSource
from .excel_data_source import ExcelDataSource
from .redshift_data_source import RedshiftDataSource

logger = logging.getLogger(__name__)

_data_source_instance: Optional[TrackmanDataSource] = None


def get_data_source() -> TrackmanDataSource:
    """
    Get the appropriate Trackman data source based on configuration.

    Returns:
        TrackmanDataSource instance (Redshift or Excel fallback)

    The function checks for:
    1. USE_REDSHIFT environment variable = "true"
    2. All required REDSHIFT_* environment variables exist
    3. Falls back to Excel if conditions not met

    This is a singleton - the same instance is returned on subsequent calls.
    """
    global _data_source_instance

    if _data_source_instance is not None:
        return _data_source_instance

    use_redshift = os.getenv("USE_REDSHIFT", "false").lower() == "true"

    if use_redshift:
        try:
            # Check if all required Redshift env vars are present
            required_vars = [
                "REDSHIFT_HOST",
                "REDSHIFT_DB",
                "REDSHIFT_USER",
                "REDSHIFT_PASSWORD",
            ]

            missing_vars = [var for var in required_vars if not os.getenv(var)]

            if missing_vars:
                logger.warning(
                    f"USE_REDSHIFT=true but missing environment variables: {', '.join(missing_vars)}. "
                    "Falling back to Excel data source."
                )
                _data_source_instance = ExcelDataSource()
            else:
                logger.info("Initializing Redshift data source")
                _data_source_instance = RedshiftDataSource()
                logger.info("Redshift data source active")

        except Exception as e:
            logger.error(
                f"Failed to initialize Redshift data source: {str(e)}. "
                "Falling back to Excel data source."
            )
            _data_source_instance = ExcelDataSource()
    else:
        logger.info("Initializing Excel data source (USE_REDSHIFT not set to true)")
        _data_source_instance = ExcelDataSource()
        logger.info("Excel data source active")

    return _data_source_instance


def reset_data_source():
    """Reset the data source instance (useful for testing)."""
    global _data_source_instance
    _data_source_instance = None
