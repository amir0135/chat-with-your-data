from typing import Annotated

from semantic_kernel.functions import kernel_function

from ..common.answer import Answer
from ..tools.question_answer_tool import QuestionAnswerTool
from ..tools.text_processing_tool import TextProcessingTool
from ..tools.trackman_query_tool import TrackmanQueryTool


class ChatPlugin:
    def __init__(self, question: str, chat_history: list[dict]) -> None:
        self.question = question
        self.chat_history = chat_history

    @kernel_function(
        description="Provide answers to any fact question coming from users."
    )
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
        description="Useful when you want to apply a transformation on the text, like translate, summarize, rephrase and so on."
    )
    def text_processing(
        self,
        text: Annotated[str, "The text to be processed"],
        operation: Annotated[
            str,
            "The operation to be performed on the text. Like Translate to Italian, Summarize, Paraphrase, etc. If a language is specified, return that as part of the operation. Preserve the operation name in the user language.",
        ],
    ) -> Answer:
        return TextProcessingTool().answer_question(
            question=self.question,
            chat_history=self.chat_history,
            text=text,
            operation=operation,
        )

    @kernel_function(
        description="Query Trackman operational data including facility errors, connectivity status, disconnect reasons, data quality metrics, and facility summaries. Use this when the user asks about TrackMan operations, error reports, connectivity issues, or data quality."
    )
    def query_trackman_data(
        self,
        intent: Annotated[
            str,
            "Type of query: errors_summary, top_error_messages, connectivity_summary, disconnect_reasons, facility_summary, or data_quality_summary",
        ],
        range_days: Annotated[int, "Number of days to look back (default: 30)"] = 30,
        facility_id: Annotated[
            str, "Optional facility ID to filter results (e.g., 'FAC001')"
        ] = "",
        limit: Annotated[int, "Maximum results for top queries (default: 10)"] = 10,
    ) -> Answer:
        return TrackmanQueryTool().query_trackman(
            intent=intent,
            range_days=range_days,
            facility_id=facility_id,
            limit=limit,
        )
