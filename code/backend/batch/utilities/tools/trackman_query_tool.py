"""Trackman query tool for accessing operational data."""

import json
import logging
from typing import Annotated

from semantic_kernel.functions import kernel_function

from ..common.answer import Answer
from ..helpers.trackman.data_source_factory import get_data_source

logger = logging.getLogger(__name__)


class TrackmanQueryTool:
    """Tool for querying Trackman operational data."""

    VALID_INTENTS = [
        "errors_summary",
        "top_error_messages",
        "connectivity_summary",
        "disconnect_reasons",
        "facility_summary",
        "data_quality_summary",
    ]

    @kernel_function(
        description=(
            "Query Trackman operational data including errors, connectivity, "
            "facility information, and data quality metrics. "
            "Use this when the user asks about TrackMan facility operations, "
            "error reports, connectivity issues, or data quality."
        )
    )
    def query_trackman(
        self,
        intent: Annotated[
            str,
            "The type of query to perform. Must be one of: "
            "errors_summary, top_error_messages, connectivity_summary, "
            "disconnect_reasons, facility_summary, data_quality_summary",
        ],
        range_days: Annotated[
            int, "Number of days to look back for time-based queries (default: 30)"
        ] = 30,
        facility_id: Annotated[
            str, "Optional facility ID to filter results (e.g., 'FAC001')"
        ] = "",
        limit: Annotated[int, "Maximum number of results to return (default: 10)"] = 10,
    ) -> Answer:
        """
        Execute a Trackman data query.

        Args:
            intent: Type of query (e.g., 'errors_summary', 'facility_summary')
            range_days: Number of days to look back (default 30)
            facility_id: Optional facility ID filter
            limit: Maximum results to return (for top_error_messages)

        Returns:
            Answer object with query results
        """
        try:
            # Validate intent
            if intent not in self.VALID_INTENTS:
                error_msg = (
                    f"Invalid intent '{intent}'. Must be one of: {', '.join(self.VALID_INTENTS)}"
                )
                logger.error(error_msg)
                return Answer(question="", answer=error_msg, source_documents=[])

            # Get data source
            data_source = get_data_source()

            # Execute query based on intent
            facility_filter = facility_id if facility_id else None

            if intent == "errors_summary":
                result = data_source.get_errors_summary(range_days, facility_filter)

            elif intent == "top_error_messages":
                result = data_source.get_top_error_messages(
                    range_days, limit, facility_filter
                )

            elif intent == "connectivity_summary":
                result = data_source.get_connectivity_summary(range_days, facility_filter)

            elif intent == "disconnect_reasons":
                result = data_source.get_disconnect_reasons(range_days, facility_filter)

            elif intent == "facility_summary":
                if not facility_id:
                    return Answer(
                        question="",
                        answer="facility_summary requires a facility_id parameter",
                        source_documents=[],
                    )
                result = data_source.get_facility_summary(facility_id, range_days)

            elif intent == "data_quality_summary":
                result = data_source.get_data_quality_summary(range_days, facility_filter)

            # Format result as answer
            answer_text = self._format_result(result, intent)

            return Answer(
                question="",
                answer=answer_text,
                source_documents=[],
                prompt_tokens=0,
                completion_tokens=0,
            )

        except Exception as e:
            error_msg = f"Error executing Trackman query: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return Answer(question="", answer=error_msg, source_documents=[])

    def _format_result(self, result: dict, intent: str) -> str:
        """Format query result as readable text with table."""
        try:
            metadata = result.get("metadata", {})
            columns = result.get("columns", [])
            rows = result.get("rows", [])

            if not rows:
                return "No data found for the specified criteria."

            # Build summary text
            source = metadata.get("source", "unknown")
            row_count = metadata.get("rowCount", len(rows))

            summary_lines = [
                f"**Trackman {intent.replace('_', ' ').title()} Report**",
                f"Source: {source}",
                f"Results: {row_count} rows",
            ]

            if "range_days" in metadata:
                summary_lines.append(f"Time range: Last {metadata['range_days']} days")

            if metadata.get("facility_id"):
                summary_lines.append(f"Facility: {metadata['facility_id']}")

            summary_lines.append("")  # Empty line before table

            # Build markdown table
            if columns and rows:
                # Header row
                header = "| " + " | ".join(str(col) for col in columns) + " |"
                separator = "|" + "|".join("---" for _ in columns) + "|"

                table_lines = [header, separator]

                # Data rows
                for row in rows:
                    row_str = "| " + " | ".join(str(cell) for cell in row) + " |"
                    table_lines.append(row_str)

                summary_lines.extend(table_lines)

            return "\n".join(summary_lines)

        except Exception as e:
            logger.error(f"Error formatting result: {str(e)}")
            return f"Data retrieved but formatting error occurred: {str(e)}\n\nRaw data: {json.dumps(result, indent=2)}"
