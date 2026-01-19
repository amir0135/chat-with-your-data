# Copilot Instructions for Chat With Your Data Solution Accelerator

## Architecture Overview

This is a **RAG (Retrieval-Augmented Generation) solution** for Azure OpenAI with three main deployable services:

| Service | Path | Technology | Purpose |
|---------|------|------------|---------|
| **web** | `code/` | Flask | Main chat API serving the React frontend |
| **adminweb** | `code/backend/` | Streamlit | Document ingestion/configuration UI (pages: `01_Ingest_Data.py`, `02_Explore_Data.py`, etc.) |
| **function** | `code/backend/batch/` | Azure Functions | Background processing (embeddings, batch indexing) |

### Key Data Flow
1. Documents uploaded via Admin UI → stored in Azure Blob Storage
2. Azure Functions process documents → chunk → embed via Azure OpenAI → index in Azure AI Search (or PostgreSQL)
3. Chat queries hit Flask API → orchestrator retrieves context from search → LLM generates response

## Critical Configuration Patterns

### Environment Variables (`EnvHelper`)
All configuration loads through the singleton `EnvHelper` class at [code/backend/batch/utilities/helpers/env_helper.py](code/backend/batch/utilities/helpers/env_helper.py). Key patterns:
- Uses Azure Key Vault for secrets (via `SecretHelper`)
- Supports both `AZURE_AUTH_TYPE=keys` and `AZURE_AUTH_TYPE=rbac`
- Database type (`DATABASE_TYPE`) switches between `CosmosDB` (default) and `PostgreSQL`

### Conversation Flow Modes
Set via `CONVERSATION_FLOW` environment variable:
- `custom` - Uses pluggable orchestrators (Semantic Kernel, LangChain, OpenAI Functions)
- `byod` - Mimics Azure OpenAI "On Your Data" pattern

### Orchestration Strategies
Located in `code/backend/batch/utilities/orchestrator/`. The `ORCHESTRATION_STRATEGY` env var selects:
- `openai_function` - OpenAI function calling
- `langchain` - LangChain agent
- `semantic_kernel` - Microsoft Semantic Kernel
- `prompt_flow` - Azure ML Prompt Flow

## Build & Test Commands

```bash
# Install dependencies
poetry install                    # Python deps
cd code/frontend && npm install   # Frontend deps

# Run tests
make unittest                     # Python unit tests (excludes azure, functional markers)
make unittest-frontend            # Frontend Jest tests
make functionaltest               # Functional tests (require running server)
make lint                         # flake8 linting

# Local development
./start_local.sh                  # Starts Flask (5050) + Vite (5173) + PostgreSQL container
./stop_local.sh                   # Cleanup

# Azure deployment
azd provision                     # Provision Azure resources
azd deploy web|adminweb|function  # Deploy individual services
```

## Test Markers (pytest.ini)
Use markers to run specific test categories:
- `@pytest.mark.unittest` - Fast unit tests
- `@pytest.mark.functional` - Tests requiring stubbed server
- `@pytest.mark.azure` - Extended tests hitting real Azure services

## Project Conventions

### Import Paths
Python imports use `code/` as the root (configured via `pythonpath = ./code` in pytest.ini):
```python
from backend.batch.utilities.helpers.env_helper import EnvHelper
from backend.batch.utilities.orchestrator.strategies import get_orchestrator
```

### Search Handler Pattern
The `Search.get_search_handler()` factory in [code/backend/batch/utilities/search/search.py](code/backend/batch/utilities/search/search.py) selects the appropriate handler based on database type and integrated vectorization settings.

### Document Processing Pipeline
Located in `code/backend/batch/utilities/`:
- `document_loading/` - File type handlers
- `document_chunking/` - Chunking strategies
- `integrated_vectorization/` - Azure AI Search native vectorization

### Configuration Storage
Runtime config stored in Azure Blob Storage (`config` container, `active.json` file). Schema defined in [code/backend/batch/utilities/helpers/config/config_helper.py](code/backend/batch/utilities/helpers/config/config_helper.py).

## Azure Deployment (azure.yaml)
Uses Azure Developer CLI (`azd`). The three services map to:
- `web` → Azure App Service (Flask + bundled React frontend)
- `adminweb` → Azure App Service (Streamlit)
- `function` → Azure Functions

Pre-package hooks handle frontend bundling (`scripts/package_frontend.sh`) and requirements export.

## Key Files Reference
- Entry points: [code/app.py](code/app.py), [code/backend/Admin.py](code/backend/Admin.py), [code/backend/batch/function_app.py](code/backend/batch/function_app.py)
- Flask routes: [code/create_app.py](code/create_app.py)
- Azure Functions: `code/backend/batch/*.py` (registered as blueprints)
- Infrastructure: `infra/main.bicep` (sandbox: `main.parameters.json`, production: `main.waf.parameters.json`)
