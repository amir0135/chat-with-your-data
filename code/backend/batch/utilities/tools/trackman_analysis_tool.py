"""Multi-step analysis tool for complex Trackman queries.

This tool performs coordinated multi-query analysis, combining results
from multiple data sources to answer complex analytical questions.
"""

import logging
from typing import Dict, List, Optional

from ..common.answer import Answer
from ..helpers.llm_helper import LLMHelper
from ..helpers.trackman.data_source_factory import get_data_source
from .trackman_query_tool import TrackmanQueryTool
from .trackman_nl_query_tool import TrackmanNLQueryTool

logger = logging.getLogger(__name__)


# Analysis prompt for synthesizing multi-query results
SYNTHESIS_PROMPT = """You are a data analyst assistant. You have been given results from multiple database queries about Trackman facility operations.

Your task is to:
1. Analyze the data from each query
2. Find correlations and patterns across the datasets
3. Provide actionable insights
4. Answer the user's original question with specific data points

Keep your response concise and data-driven. Use bullet points for key findings.
Format numbers clearly (e.g., "85%" not "0.85").

User's Question: {question}

Query Results:
{query_results}

Provide your analysis:"""


class TrackmanAnalysisTool:
    """Tool for complex multi-step Trackman data analysis.

    This tool coordinates multiple queries and synthesizes results
    to answer complex analytical questions that require data from
    multiple sources or time comparisons.
    """

    def __init__(self):
        self.llm_helper = LLMHelper()
        self.query_tool = TrackmanQueryTool()
        self.nl_query_tool = TrackmanNLQueryTool()
        self.data_source = None

    def _get_data_source(self):
        """Lazy-load data source."""
        if self.data_source is None:
            self.data_source = get_data_source()
        return self.data_source

    def _run_intent_query(
        self, intent: str, range_days: int, facility_id: str = "", limit: int = 10
    ) -> Dict:
        """Run a predefined intent-based query."""
        try:
            data_source = self._get_data_source()

            if intent == "errors_summary":
                return data_source.get_errors_summary(range_days, facility_id or None)
            elif intent == "top_error_messages":
                return data_source.get_top_error_messages(
                    range_days, limit, facility_id or None
                )
            elif intent == "connectivity_summary":
                return data_source.get_connectivity_summary(
                    range_days, facility_id or None
                )
            elif intent == "disconnect_reasons":
                return data_source.get_disconnect_reasons(
                    range_days, facility_id or None
                )
            elif intent == "facility_summary":
                if not facility_id:
                    return {"error": "facility_id required for facility_summary"}
                return data_source.get_facility_summary(facility_id, range_days)
            elif intent == "data_quality_summary":
                return data_source.get_data_quality_summary(
                    range_days, facility_id or None
                )
            else:
                return {"error": f"Unknown intent: {intent}"}
        except Exception as e:
            logger.error(f"Error running intent query {intent}: {str(e)}")
            return {"error": str(e)}

    def _format_query_result(self, result: Dict, query_name: str) -> str:
        """Format a single query result as text for synthesis."""
        if "error" in result:
            return f"**{query_name}**: Error - {result['error']}"

        columns = result.get("columns", [])
        rows = result.get("rows", [])

        if not rows:
            return f"**{query_name}**: No data found"

        # Format as simple table for LLM consumption
        lines = [f"**{query_name}** ({len(rows)} rows):"]
        lines.append(" | ".join(str(c) for c in columns))
        lines.append("-" * 40)

        for row in rows[:20]:  # Limit rows for synthesis
            lines.append(" | ".join(str(v) if v is not None else "" for v in row))

        if len(rows) > 20:
            lines.append(f"... and {len(rows) - 20} more rows")

        return "\n".join(lines)

    def _synthesize_results(self, question: str, query_results: List[Dict]) -> Dict:
        """Use LLM to synthesize multiple query results into insights."""
        formatted_results = "\n\n".join(
            self._format_query_result(r["data"], r["name"]) for r in query_results
        )

        messages = [
            {"role": "system", "content": "You are a helpful data analyst."},
            {
                "role": "user",
                "content": SYNTHESIS_PROMPT.format(
                    question=question,
                    query_results=formatted_results,
                ),
            },
        ]

        response = self.llm_helper.get_chat_completion(messages)

        return {
            "synthesis": response.choices[0].message.content,
            "prompt_tokens": response.usage.prompt_tokens,
            "completion_tokens": response.usage.completion_tokens,
        }

    def analyze_facility_health(self, facility_id: str, range_days: int = 30) -> Answer:
        """
        Comprehensive health analysis for a specific facility.

        Queries:
        1. Error summary
        2. Top error messages
        3. Connectivity summary
        4. Data quality

        Then synthesizes insights.
        """
        question = f"Analyze the health of facility {facility_id} over the last {range_days} days"
        total_prompt_tokens = 0
        total_completion_tokens = 0

        try:
            # Run multiple queries
            queries = [
                {
                    "name": "Error Summary",
                    "data": self._run_intent_query(
                        "errors_summary", range_days, facility_id
                    ),
                },
                {
                    "name": "Top Errors",
                    "data": self._run_intent_query(
                        "top_error_messages", range_days, facility_id, limit=5
                    ),
                },
                {
                    "name": "Connectivity",
                    "data": self._run_intent_query(
                        "connectivity_summary", range_days, facility_id
                    ),
                },
                {
                    "name": "Data Quality",
                    "data": self._run_intent_query(
                        "data_quality_summary", range_days, facility_id
                    ),
                },
            ]

            # Synthesize results
            synthesis = self._synthesize_results(question, queries)
            total_prompt_tokens += synthesis["prompt_tokens"]
            total_completion_tokens += synthesis["completion_tokens"]

            return Answer(
                question=question,
                answer=synthesis["synthesis"],
                source_documents=[],
                prompt_tokens=total_prompt_tokens,
                completion_tokens=total_completion_tokens,
            )

        except Exception as e:
            logger.error(f"Error in facility health analysis: {str(e)}", exc_info=True)
            return Answer(
                question=question,
                answer=f"Error performing analysis: {str(e)}",
                source_documents=[],
            )

    def compare_facilities(
        self, facility_ids: List[str], range_days: int = 30
    ) -> Answer:
        """
        Compare multiple facilities across key metrics.
        """
        question = f"Compare facilities {', '.join(facility_ids)} over the last {range_days} days"
        total_prompt_tokens = 0
        total_completion_tokens = 0

        try:
            queries = []

            for fac_id in facility_ids[:5]:  # Limit to 5 facilities
                queries.append(
                    {
                        "name": f"{fac_id} - Errors",
                        "data": self._run_intent_query(
                            "errors_summary", range_days, fac_id
                        ),
                    }
                )
                queries.append(
                    {
                        "name": f"{fac_id} - Connectivity",
                        "data": self._run_intent_query(
                            "connectivity_summary", range_days, fac_id
                        ),
                    }
                )

            # Synthesize comparison
            synthesis = self._synthesize_results(question, queries)
            total_prompt_tokens += synthesis["prompt_tokens"]
            total_completion_tokens += synthesis["completion_tokens"]

            return Answer(
                question=question,
                answer=synthesis["synthesis"],
                source_documents=[],
                prompt_tokens=total_prompt_tokens,
                completion_tokens=total_completion_tokens,
            )

        except Exception as e:
            logger.error(f"Error in facility comparison: {str(e)}", exc_info=True)
            return Answer(
                question=question,
                answer=f"Error performing comparison: {str(e)}",
                source_documents=[],
            )

    def analyze_trend(
        self, metric: str, range_days: int = 30, facility_id: str = ""
    ) -> Answer:
        """
        Analyze trends for a specific metric over time.

        Compares current period vs previous period to identify changes.
        """
        question = f"Analyze {metric} trends over {range_days} days" + (
            f" for facility {facility_id}" if facility_id else ""
        )
        total_prompt_tokens = 0
        total_completion_tokens = 0

        try:
            # Get current period data
            if metric == "errors":
                current = self._run_intent_query(
                    "errors_summary", range_days, facility_id
                )
                previous = self._run_intent_query(
                    "errors_summary", range_days * 2, facility_id
                )
            elif metric == "connectivity":
                current = self._run_intent_query(
                    "connectivity_summary", range_days, facility_id
                )
                previous = self._run_intent_query(
                    "connectivity_summary", range_days * 2, facility_id
                )
            else:
                current = self._run_intent_query(
                    "data_quality_summary", range_days, facility_id
                )
                previous = self._run_intent_query(
                    "data_quality_summary", range_days * 2, facility_id
                )

            queries = [
                {"name": f"Current Period ({range_days} days)", "data": current},
                {"name": f"Extended Period ({range_days * 2} days)", "data": previous},
            ]

            # Synthesize trend analysis
            synthesis = self._synthesize_results(
                question
                + ". Compare current period to previous period and identify trends.",
                queries,
            )
            total_prompt_tokens += synthesis["prompt_tokens"]
            total_completion_tokens += synthesis["completion_tokens"]

            return Answer(
                question=question,
                answer=synthesis["synthesis"],
                source_documents=[],
                prompt_tokens=total_prompt_tokens,
                completion_tokens=total_completion_tokens,
            )

        except Exception as e:
            logger.error(f"Error in trend analysis: {str(e)}", exc_info=True)
            return Answer(
                question=question,
                answer=f"Error performing trend analysis: {str(e)}",
                source_documents=[],
            )

    def correlate_errors_connectivity(
        self, range_days: int = 30, facility_id: str = ""
    ) -> Answer:
        """
        Analyze correlation between errors and connectivity issues.

        Useful for identifying if connectivity problems cause errors.
        """
        question = "Analyze correlation between errors and connectivity issues" + (
            f" for facility {facility_id}" if facility_id else ""
        )
        total_prompt_tokens = 0
        total_completion_tokens = 0

        try:
            queries = [
                {
                    "name": "Errors",
                    "data": self._run_intent_query(
                        "errors_summary", range_days, facility_id
                    ),
                },
                {
                    "name": "Top Errors",
                    "data": self._run_intent_query(
                        "top_error_messages", range_days, facility_id, limit=10
                    ),
                },
                {
                    "name": "Connectivity",
                    "data": self._run_intent_query(
                        "connectivity_summary", range_days, facility_id
                    ),
                },
                {
                    "name": "Disconnect Reasons",
                    "data": self._run_intent_query(
                        "disconnect_reasons", range_days, facility_id
                    ),
                },
            ]

            synthesis = self._synthesize_results(
                question
                + ". Look for patterns: do high error counts correlate with connectivity issues?",
                queries,
            )
            total_prompt_tokens += synthesis["prompt_tokens"]
            total_completion_tokens += synthesis["completion_tokens"]

            return Answer(
                question=question,
                answer=synthesis["synthesis"],
                source_documents=[],
                prompt_tokens=total_prompt_tokens,
                completion_tokens=total_completion_tokens,
            )

        except Exception as e:
            logger.error(f"Error in correlation analysis: {str(e)}", exc_info=True)
            return Answer(
                question=question,
                answer=f"Error performing correlation analysis: {str(e)}",
                source_documents=[],
            )
