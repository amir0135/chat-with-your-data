import json
import logging
import os
import sys
import pytest
from pytest_httpserver import HTTPServer

from tests.functional.app_config import AppConfig
from tests.constants import (
    AZURE_STORAGE_CONFIG_CONTAINER_NAME,
    AZURE_STORAGE_CONFIG_FILE_NAME,
)

# Import from BOTH paths to ensure we clear both singletons
# The production code may be imported from either path depending on how Python resolves modules
from backend.batch.utilities.helpers.config.config_helper import ConfigHelper as ConfigHelper1
from backend.batch.utilities.helpers.env_helper import EnvHelper as EnvHelper1

sys.path.append(
    os.path.join(os.path.dirname(sys.path[0]), "..", "..", "..", "backend", "batch")
)
from utilities.helpers.config.config_helper import ConfigHelper as ConfigHelper2  # noqa: E402
from utilities.helpers.env_helper import EnvHelper as EnvHelper2  # noqa: E402

logger = logging.getLogger(__name__)


@pytest.fixture(scope="package")
def app_config(make_httpserver, ca):
    logger.info("Creating APP CONFIG")
    with ca.cert_pem.tempfile() as ca_temp_path:
        app_config = AppConfig(
            {
                "AZURE_OPENAI_ENDPOINT": f"https://localhost:{make_httpserver.port}/",
                "AZURE_SEARCH_SERVICE": f"https://localhost:{make_httpserver.port}/",
                "AZURE_CONTENT_SAFETY_ENDPOINT": f"https://localhost:{make_httpserver.port}/",
                "AZURE_SPEECH_REGION_ENDPOINT": f"https://localhost:{make_httpserver.port}/",
                "AZURE_STORAGE_ACCOUNT_ENDPOINT": f"https://localhost:{make_httpserver.port}/",
                "AZURE_SEARCH_USE_INTEGRATED_VECTORIZATION": "True",
                "APP_ENV": "dev",  # Use dev mode to skip identity field in datasource/skillset/index
                "SSL_CERT_FILE": ca_temp_path,
                "CURL_CA_BUNDLE": ca_temp_path,
            }
        )
        logger.info(f"Created app config: {app_config.get_all()}")
        yield app_config


@pytest.fixture(scope="package", autouse=True)
def manage_app(app_config: AppConfig):
    logger.info("[IV manage_app] Starting fixture setup")
    app_config.apply_to_environment()
    logger.info("[IV manage_app] After apply_to_environment")

    # Clear BOTH EnvHelper singletons (from different import paths)
    EnvHelper1.clear_instance()
    EnvHelper2.clear_instance()
    logger.info("[IV manage_app] After EnvHelper.clear_instance() (both paths)")

    # Clear BOTH ConfigHelper caches
    ConfigHelper1.clear_config()
    ConfigHelper2.clear_config()
    logger.info("[IV manage_app] After ConfigHelper.clear_config() (both paths)")

    # Verify state after clearing
    import os
    iv_env = os.environ.get("AZURE_SEARCH_USE_INTEGRATED_VECTORIZATION", "NOT SET")
    logger.info(f"[IV manage_app] Env var AZURE_SEARCH_USE_INTEGRATED_VECTORIZATION={iv_env}")

    # Check if EnvHelper singleton exists
    logger.info(f"[IV manage_app] EnvHelper1._instance is None: {EnvHelper1._instance is None}")
    logger.info(f"[IV manage_app] EnvHelper2._instance is None: {EnvHelper2._instance is None}")

    yield

    logger.info("[IV manage_app] Starting fixture teardown")
    app_config.remove_from_environment()
    EnvHelper1.clear_instance()
    EnvHelper2.clear_instance()
    ConfigHelper1.clear_config()
    ConfigHelper2.clear_config()
    logger.info("[IV manage_app] Fixture teardown complete")


# Override the default setup_config_mocking fixture to include integrated_vectorization_config
@pytest.fixture(autouse=True)
def setup_config_mocking(httpserver: HTTPServer):
    """Override the default config mocking to include integrated_vectorization_config for IV tests."""
    logger.info("[IV setup_config_mocking] Setting up config with integrated_vectorization_config")
    config_data = {
        "prompts": {
            "condense_question_prompt": "",
            "answering_system_prompt": "system prompt",
            "answering_user_prompt": "## Retrieved Documents\n{sources}\n\n## User Question\nUse the Retrieved Documents to answer the question: {question}",
            "use_on_your_data_format": True,
            "post_answering_prompt": "post answering prompt\n{question}\n{answer}\n{sources}",
            "enable_post_answering_prompt": False,
            "enable_content_safety": True,
        },
        "messages": {"post_answering_filter": "post answering filter"},
        "example": {
            "documents": '{"retrieved_documents":[{"[doc1]":{"content":"content"}}]}',
            "user_question": "user question",
            "answer": "answer",
        },
        "document_processors": [
            {
                "document_type": "pdf",
                "chunking": {"strategy": "layout", "size": 500, "overlap": 100},
                "loading": {"strategy": "layout"},
                "use_advanced_image_processing": False,
            },
        ],
        "logging": {"log_user_interactions": True, "log_tokens": True},
        "orchestrator": {"strategy": "openai_function"},
        "integrated_vectorization_config": {
            "max_page_length": 2000,
            "page_overlap_length": 500,
        },
        "enable_chat_history": True,
    }

    config_json_string = json.dumps(config_data)
    logger.info(f"[IV setup_config_mocking] Config JSON length: {len(config_json_string)} bytes")

    httpserver.expect_request(
        f"/{AZURE_STORAGE_CONFIG_CONTAINER_NAME}/{AZURE_STORAGE_CONFIG_FILE_NAME}",
        method="GET",
    ).respond_with_data(
        config_json_string,
        headers={
            "Content-Type": "application/json",
            "Content-Range": f"bytes 0-{len(config_json_string.encode('utf-8'))-1}/{len(config_json_string.encode('utf-8'))}",
        },
    )

    yield
