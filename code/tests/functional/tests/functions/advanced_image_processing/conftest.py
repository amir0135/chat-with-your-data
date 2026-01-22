import logging
import os
import sys
import pytest
from tests.functional.app_config import AppConfig

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
                "AZURE_COMPUTER_VISION_ENDPOINT": f"https://localhost:{make_httpserver.port}/",
                "USE_ADVANCED_IMAGE_PROCESSING": "True",
                "SSL_CERT_FILE": ca_temp_path,
                "CURL_CA_BUNDLE": ca_temp_path,
            }
        )
        logger.info(f"Created app config: {app_config.get_all()}")
        yield app_config


@pytest.fixture(scope="package", autouse=True)
def manage_app(app_config: AppConfig):
    app_config.apply_to_environment()
    # Clear both singletons from both module paths to handle Python's dual-import issue
    EnvHelper1.clear_instance()
    EnvHelper2.clear_instance()
    ConfigHelper1.clear_config()
    ConfigHelper2.clear_config()
    yield
    app_config.remove_from_environment()
    EnvHelper1.clear_instance()
    EnvHelper2.clear_instance()
    ConfigHelper1.clear_config()
    ConfigHelper2.clear_config()
