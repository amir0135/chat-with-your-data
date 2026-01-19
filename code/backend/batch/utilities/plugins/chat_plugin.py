from typing import Annotated

from semantic_kernel.functions import kernel_function

from ..common.answer import Answer
from ..tools.question_answer_tool import QuestionAnswerTool
from ..tools.text_processing_tool import TextProcessingTool
from ..tools.trackman_nl_query_tool import TrackmanNLQueryTool
from ..tools.trackman_analysis_tool import TrackmanAnalysisTool
from ..helpers.azure_ai_integration import trace_operation


class ChatPlugin:
    """
    Simplified ChatPlugin with 4 tools:
    1. search_documents - RAG for uploaded docs
    2. text_processing - Text transformations
    3. query_trackman - All database queries (LLM-generated SQL)
    4. analyze_trackman - Multi-step analysis

    Enhanced with Azure AI Tracing for observability.
    """

    def __init__(self, question: str, chat_history: list[dict]) -> None:
        self.question = question
        self.chat_history = chat_history

    @kernel_function(
        description="Search uploaded documents and knowledge base. Use ONLY for: documentation, procedures, policies, how-to guides, or file contents. Do NOT use for data queries about errors, sessions, facilities, or performance metrics."
    )
    @trace_operation("search_documents", {"plugin": "chat"})
    def search_documents(
        self,
        question: Annotated[
            str, "A standalone question, converted from the chat history"
        ],
    ) -> Answer:
        return QuestionAnswerTool().answer_question(
            question=question, chat_history=self.chat_history
        )

    @kernel_function(
        description="Apply text transformations like translate, summarize, rephrase."
    )
    @trace_operation("text_processing", {"plugin": "chat"})
    def text_processing(
        self,
        text: Annotated[str, "The text to be processed"],
        operation: Annotated[
            str,
            "The operation to perform: Translate to Italian, Summarize, Paraphrase, etc.",
        ],
    ) -> Answer:
        return TextProcessingTool().answer_question(
            question=self.question,
            chat_history=self.chat_history,
            text=text,
            operation=operation,
        )

    @kernel_function(
        description="Query Trackman/operational database for data counts and metrics. Use for: error counts, error types, session counts, disconnection counts, connectivity metrics, facility performance, bay performance, unit statistics, or any question asking 'how many', 'which has most/least', 'top N', 'count of', 'list of errors/sessions/facilities'."
    )
    @trace_operation("query_trackman", {"plugin": "chat", "data_source": "trackman"})
    def query_trackman(
        self,
        question: Annotated[
            str,
            "The question exactly as asked, e.g., 'Which facilities have the most errors?'",
        ],
        max_rows: Annotated[int, "Maximum rows to return (default: 50, max: 100)"] = 50,
    ) -> Answer:
        return TrackmanNLQueryTool().query_with_natural_language(
            question=question,
            max_rows=min(max_rows, 100),
        )

    @kernel_function(
        description="Deep analysis of Trackman data. Use for: facility health assessment, comparing multiple facilities, trend analysis over time, or correlating errors with connectivity issues."
    )
    @trace_operation("analyze_trackman", {"plugin": "chat", "data_source": "trackman"})
    def analyze_trackman(
        self,
        analysis_type: Annotated[
            str,
            "'facility_health', 'compare_facilities', 'trend', or 'correlation'",
        ],
        facility_id: Annotated[
            str,
            "Facility name or ID for analysis",
        ] = "",
        facility_ids: Annotated[
            str,
            "Comma-separated facility names/IDs for comparison",
        ] = "",
        metric: Annotated[
            str,
            "'errors', 'connectivity', or 'data_quality'",
        ] = "errors",
        range_days: Annotated[int, "Days to analyze (default: 30)"] = 30,
    ) -> Answer:
        tool = TrackmanAnalysisTool()

        if analysis_type == "facility_health":
            if not facility_id:
                return Answer(
                    question="Facility health analysis",
                    answer="Error: facility_id is required for facility_health analysis",
                    source_documents=[],
                )
            return tool.analyze_facility_health(facility_id, range_days)

        elif analysis_type == "compare_facilities":
            if not facility_ids:
                return Answer(
                    question="Facility comparison",
                    answer="Error: facility_ids (comma-separated) is required for comparison",
                    source_documents=[],
                )
            fac_list = [f.strip() for f in facility_ids.split(",") if f.strip()]
            return tool.compare_facilities(fac_list, range_days)

        elif analysis_type == "trend":
            return tool.analyze_trend(metric, range_days, facility_id)

        elif analysis_type == "correlation":
            return tool.correlate_errors_connectivity(range_days, facility_id)

        else:
            return Answer(
                question=f"Analysis: {analysis_type}",
                answer=f"Unknown analysis type: {analysis_type}. Use 'facility_health', 'compare_facilities', 'trend', or 'correlation'.",
                source_documents=[],
            )
