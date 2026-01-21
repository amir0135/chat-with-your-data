#!/bin/bash
# =============================================================================
# Local Development Setup Script
# =============================================================================
# This script configures your local environment after deploying to Azure.
# It pulls configuration from your Azure deployment and sets up all
# necessary files for local development including TrackMan integration.
#
# Prerequisites:
#   - Azure CLI installed and logged in (az login)
#   - Azure Developer CLI installed (azd)
#   - azd up completed successfully
#   - Docker installed (for PostgreSQL container)
#   - Node.js 18+ and npm installed
#   - Python 3.11+ with Poetry installed
#
# Usage:
#   ./scripts/setup_local.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$ROOT_DIR"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║     Chat With Your Data - Local Environment Setup          ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# =============================================================================
# Step 1: Check Prerequisites
# =============================================================================
echo -e "${BLUE}[1/7] Checking prerequisites...${NC}"

# Check Azure CLI
if ! command -v az &> /dev/null; then
    echo -e "${RED}✗ Azure CLI not found. Install from: https://docs.microsoft.com/cli/azure/install-azure-cli${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Azure CLI installed${NC}"

# Check azd
if ! command -v azd &> /dev/null; then
    echo -e "${RED}✗ Azure Developer CLI not found. Install from: https://learn.microsoft.com/azure/developer/azure-developer-cli/install-azd${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Azure Developer CLI installed${NC}"

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker not found. Install Docker Desktop from: https://www.docker.com/products/docker-desktop${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker installed${NC}"

# Check Node.js
if ! command -v node &> /dev/null; then
    echo -e "${RED}✗ Node.js not found. Install from: https://nodejs.org${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Node.js $(node --version) installed${NC}"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}✗ Python 3 not found. Install Python 3.11+${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Python $(python3 --version | cut -d' ' -f2) installed${NC}"

# Check Poetry
if ! command -v poetry &> /dev/null; then
    echo -e "${RED}✗ Poetry not found. Install from: https://python-poetry.org/docs/#installation${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Poetry installed${NC}"

# Check Azure Functions Core Tools
if ! command -v func &> /dev/null; then
    echo -e "${YELLOW}⚠ Azure Functions Core Tools not found. Installing...${NC}"
    npm install -g azure-functions-core-tools@4 --unsafe-perm true
fi
echo -e "${GREEN}✓ Azure Functions Core Tools installed${NC}"

echo ""

# =============================================================================
# Step 2: Check Azure Login
# =============================================================================
echo -e "${BLUE}[2/7] Checking Azure authentication...${NC}"

if ! az account show &> /dev/null; then
    echo -e "${YELLOW}Not logged in to Azure. Running 'az login'...${NC}"
    az login
fi

SUBSCRIPTION=$(az account show --query name -o tsv)
echo -e "${GREEN}✓ Logged in to Azure subscription: $SUBSCRIPTION${NC}"
echo ""

# =============================================================================
# Step 3: Get Azure Environment Variables from azd
# =============================================================================
echo -e "${BLUE}[3/7] Retrieving Azure configuration from azd...${NC}"

# Check if azd environment exists
if ! azd env list 2>/dev/null | grep -q "true"; then
    echo -e "${RED}✗ No azd environment found. Please run 'azd up' first to deploy to Azure.${NC}"
    echo -e "${YELLOW}  After deployment, run this script again.${NC}"
    exit 1
fi

# Get environment name
AZD_ENV=$(azd env list --output json 2>/dev/null | python3 -c "import sys,json; envs=json.load(sys.stdin); print(next((e['Name'] for e in envs if e.get('IsDefault')), ''))" 2>/dev/null || echo "")

if [ -z "$AZD_ENV" ]; then
    echo -e "${YELLOW}Could not detect azd environment. Using environment variables from azd env get-values...${NC}"
fi

echo -e "${GREEN}✓ Found azd environment: ${AZD_ENV:-default}${NC}"

# Export azd environment variables
echo "Loading environment variables from Azure deployment..."
eval "$(azd env get-values 2>/dev/null | grep -E '^[A-Z]' | sed 's/^/export /')"

# Verify key variables
if [ -z "$AZURE_STORAGE_ACCOUNT" ] && [ -z "$AZURE_BLOB_ACCOUNT_NAME" ]; then
    echo -e "${RED}✗ Could not retrieve Azure configuration. Make sure 'azd up' completed successfully.${NC}"
    exit 1
fi

# Use AZURE_STORAGE_ACCOUNT if AZURE_BLOB_ACCOUNT_NAME is not set
STORAGE_ACCOUNT="${AZURE_BLOB_ACCOUNT_NAME:-$AZURE_STORAGE_ACCOUNT}"
echo -e "${GREEN}✓ Storage Account: $STORAGE_ACCOUNT${NC}"
echo ""

# =============================================================================
# Step 4: Generate .env file
# =============================================================================
echo -e "${BLUE}[4/7] Generating .env file...${NC}"

if [ -f ".env" ]; then
    echo -e "${YELLOW}Backing up existing .env to .env.backup${NC}"
    cp .env .env.backup
fi

# Get values from azd or use defaults
cat > .env << EOF
# =============================================================================
# Azure Configuration (auto-generated from azd deployment)
# Generated on: $(date)
# =============================================================================

# Azure Resource Configuration
AZURE_SUBSCRIPTION_ID=${AZURE_SUBSCRIPTION_ID:-}
AZURE_RESOURCE_GROUP=${AZURE_RESOURCE_GROUP:-}
AZURE_ENV_NAME=${AZURE_ENV_NAME:-}

# Storage
AZURE_BLOB_ACCOUNT_NAME=${STORAGE_ACCOUNT}
AZURE_BLOB_CONTAINER_NAME=${AZURE_BLOB_CONTAINER_NAME:-documents}

# AI Services
AZURE_OPENAI_RESOURCE=${AZURE_OPENAI_RESOURCE:-}
AZURE_OPENAI_MODEL=${AZURE_OPENAI_MODEL:-gpt-4o}
AZURE_OPENAI_MODEL_NAME=${AZURE_OPENAI_MODEL_NAME:-gpt-4o}
AZURE_OPENAI_MODEL_VERSION=${AZURE_OPENAI_MODEL_VERSION:-}
AZURE_OPENAI_TEMPERATURE=0
AZURE_OPENAI_TOP_P=1
AZURE_OPENAI_MAX_TOKENS=1000
AZURE_OPENAI_STOP_SEQUENCE=
AZURE_OPENAI_SYSTEM_MESSAGE=You are an AI assistant that helps people find information.
AZURE_OPENAI_API_VERSION=${AZURE_OPENAI_API_VERSION:-2024-02-01}
AZURE_OPENAI_STREAM=true
AZURE_OPENAI_EMBEDDING_MODEL=${AZURE_OPENAI_EMBEDDING_MODEL:-text-embedding-ada-002}
AZURE_OPENAI_EMBEDDING_MODEL_NAME=${AZURE_OPENAI_EMBEDDING_MODEL_NAME:-text-embedding-ada-002}
AZURE_OPENAI_EMBEDDING_MODEL_VERSION=${AZURE_OPENAI_EMBEDDING_MODEL_VERSION:-2}

# Form Recognizer / Document Intelligence
AZURE_FORM_RECOGNIZER_ENDPOINT=${AZURE_FORM_RECOGNIZER_ENDPOINT:-}

# Content Safety
AZURE_CONTENT_SAFETY_ENDPOINT=${AZURE_CONTENT_SAFETY_ENDPOINT:-}

# Key Vault
AZURE_KEY_VAULT_ENDPOINT=${AZURE_KEY_VAULT_ENDPOINT:-}

# Speech Services
AZURE_SPEECH_SERVICE_NAME=${AZURE_SPEECH_SERVICE_NAME:-}
AZURE_SPEECH_SERVICE_REGION=${AZURE_SPEECH_SERVICE_REGION:-}
AZURE_SPEECH_RECOGNIZER_LANGUAGES=en-US,fr-FR,de-DE,it-IT

# Managed Identity (for RBAC auth)
MANAGED_IDENTITY_CLIENT_ID=${AZURE_CLIENT_ID:-}
AZURE_CLIENT_ID=${AZURE_CLIENT_ID:-}

# Cosmos DB
DATABASE_TYPE=${DATABASE_TYPE:-CosmosDB}
AZURE_COSMOSDB_ACCOUNT_NAME=${AZURE_COSMOSDB_ACCOUNT_NAME:-}
AZURE_COSMOSDB_DATABASE_NAME=${AZURE_COSMOSDB_DATABASE_NAME:-db_conversation_history}
AZURE_COSMOSDB_CONVERSATIONS_CONTAINER_NAME=${AZURE_COSMOSDB_CONVERSATIONS_CONTAINER_NAME:-conversations}
AZURE_COSMOSDB_ENABLE_FEEDBACK=true

# Azure AI Search
AZURE_SEARCH_SERVICE=${AZURE_SEARCH_SERVICE:-}
AZURE_SEARCH_INDEX=${AZURE_SEARCH_INDEX:-}
AZURE_SEARCH_CONVERSATIONS_LOG_INDEX=conversations
AZURE_SEARCH_USE_SEMANTIC_SEARCH=false
AZURE_SEARCH_SEMANTIC_SEARCH_CONFIG=default
AZURE_SEARCH_INDEX_IS_PRECHUNKED=false
AZURE_SEARCH_TOP_K=5
AZURE_SEARCH_ENABLE_IN_DOMAIN=true
AZURE_SEARCH_FILENAME_COLUMN=filename
AZURE_SEARCH_FILTER=
AZURE_SEARCH_FIELDS_ID=id
AZURE_SEARCH_CONTENT_COLUMN=content
AZURE_SEARCH_CONTENT_VECTOR_COLUMN=content_vector
AZURE_SEARCH_TITLE_COLUMN=title
AZURE_SEARCH_FIELDS_METADATA=metadata
AZURE_SEARCH_SOURCE_COLUMN=source
AZURE_SEARCH_TEXT_COLUMN=
AZURE_SEARCH_LAYOUT_TEXT_COLUMN=
AZURE_SEARCH_CHUNK_COLUMN=chunk
AZURE_SEARCH_OFFSET_COLUMN=offset
AZURE_SEARCH_URL_COLUMN=url
AZURE_SEARCH_USE_INTEGRATED_VECTORIZATION=false

# Application Configuration
APP_ENV=Dev
ORCHESTRATION_STRATEGY=semantic_kernel
CONVERSATION_FLOW=custom
LOGLEVEL=INFO
PACKAGE_LOGGING_LEVEL=WARNING

# Image Processing
USE_ADVANCED_IMAGE_PROCESSING=false
ADVANCED_IMAGE_PROCESSING_MAX_IMAGES=1

# =============================================================================
# TrackMan Data Integration (optional)
# =============================================================================
# Set USE_REDSHIFT=true to enable database queries via natural language
# The local PostgreSQL container simulates the production Redshift database

USE_REDSHIFT=true
TRACKMAN_DATA_DIR=data/testtrack

# Local PostgreSQL settings (for testing)
REDSHIFT_HOST=localhost
REDSHIFT_PORT=5432
REDSHIFT_DB=trackman_test
REDSHIFT_USER=testuser
REDSHIFT_PASSWORD=testpassword
REDSHIFT_SCHEMA=public

# Production Redshift settings (uncomment and fill for production)
# REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
# REDSHIFT_PORT=5439
# REDSHIFT_DB=your_database
# REDSHIFT_USER=your_user
# REDSHIFT_PASSWORD=your_password
# REDSHIFT_SCHEMA=public
EOF

echo -e "${GREEN}✓ Generated .env file${NC}"
echo ""

# =============================================================================
# Step 5: Generate local.settings.json for Azure Functions
# =============================================================================
echo -e "${BLUE}[5/7] Configuring Azure Functions local.settings.json...${NC}"

# Get storage connection string
echo "Retrieving storage connection string..."
STORAGE_CONNECTION=$(az storage account show-connection-string \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "${AZURE_RESOURCE_GROUP}" \
    --query connectionString -o tsv 2>/dev/null || echo "")

if [ -z "$STORAGE_CONNECTION" ]; then
    echo -e "${YELLOW}⚠ Could not retrieve storage connection string. Trying with DefaultAzureCredential...${NC}"
    STORAGE_CONNECTION="UseDevelopmentStorage=false"
fi

cat > code/backend/batch/local.settings.json << EOF
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsStorage": "${STORAGE_CONNECTION}"
  },
  "ConnectionStrings": {}
}
EOF

echo -e "${GREEN}✓ Generated local.settings.json${NC}"
echo ""

# =============================================================================
# Step 6: Configure Azure RBAC Roles
# =============================================================================
echo -e "${BLUE}[6/7] Configuring Azure RBAC roles...${NC}"

USER_ID=$(az ad signed-in-user show --query id -o tsv 2>/dev/null || echo "")

if [ -n "$USER_ID" ] && [ -n "$STORAGE_ACCOUNT" ]; then
    echo "Assigning Storage Blob Data Contributor role..."
    az role assignment create \
        --role "Storage Blob Data Contributor" \
        --assignee "$USER_ID" \
        --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}" \
        2>/dev/null || echo "  (Role may already be assigned)"

    echo "Assigning Storage Queue Data Contributor role..."
    az role assignment create \
        --role "Storage Queue Data Contributor" \
        --assignee "$USER_ID" \
        --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT}" \
        2>/dev/null || echo "  (Role may already be assigned)"

    # Get search service name from URL
    SEARCH_SERVICE=$(echo "$AZURE_SEARCH_SERVICE" | sed 's|https://||' | sed 's|\.search\.windows\.net.*||')
    if [ -n "$SEARCH_SERVICE" ]; then
        echo "Assigning Search Index Data Contributor role..."
        az role assignment create \
            --role "Search Index Data Contributor" \
            --assignee "$USER_ID" \
            --scope "/subscriptions/${AZURE_SUBSCRIPTION_ID}/resourceGroups/${AZURE_RESOURCE_GROUP}/providers/Microsoft.Search/searchServices/${SEARCH_SERVICE}" \
            2>/dev/null || echo "  (Role may already be assigned)"
    fi
    
    echo -e "${GREEN}✓ Azure RBAC roles configured${NC}"
else
    echo -e "${YELLOW}⚠ Could not configure RBAC roles automatically. You may need to assign them manually.${NC}"
fi
echo ""

# =============================================================================
# Step 7: Install Dependencies
# =============================================================================
echo -e "${BLUE}[7/7] Installing dependencies...${NC}"

# Python dependencies
echo "Installing Python dependencies with Poetry..."
poetry install

# Frontend dependencies
echo "Installing frontend dependencies..."
cd code/frontend && npm install && cd ../..

echo -e "${GREEN}✓ Dependencies installed${NC}"
echo ""

# =============================================================================
# Setup Complete
# =============================================================================
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              Setup Complete!                                ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo ""
echo "1. Start the local development environment:"
echo -e "   ${YELLOW}./start_local.sh${NC}"
echo ""
echo "2. Open the applications:"
echo "   • Chat UI:    http://localhost:5173"
echo "   • Admin UI:   http://localhost:8501"
echo "   • API:        http://localhost:5050"
echo "   • Functions:  http://localhost:7071"
echo ""
echo "3. Upload documents via Admin UI, then ask questions in Chat UI"
echo ""
echo "4. For TrackMan queries (if enabled), try:"
echo "   • 'How many total errors occurred in the last 7 days?'"
echo "   • 'Which facility has the most errors?'"
echo ""
echo -e "${YELLOW}To stop all services: ./stop_local.sh${NC}"
echo ""
