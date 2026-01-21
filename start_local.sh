#!/bin/bash
# =============================================================================
# Local Development Startup Script
# =============================================================================
# Starts all services: Chat UI (Flask + Vite), Admin UI (Streamlit), 
# Azure Functions, and PostgreSQL (for TrackMan testing)
#
# Prerequisites:
#   - Run ./scripts/setup_local.sh first (after azd up)
#   - Or manually create .env and install dependencies
#
# Usage:
#   ./start_local.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║   Chat With Your Data - Local Development Environment      ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# =============================================================================
# Pre-flight Checks
# =============================================================================
echo -e "${BLUE}[Preflight] Checking setup...${NC}"

# Check if .env exists
if [ ! -f ".env" ]; then
    echo -e "${RED}✗ .env file not found.${NC}"
    echo -e "${YELLOW}  Run ./scripts/setup_local.sh first, or copy .env.sample to .env${NC}"
    exit 1
fi

# Check if .venv exists
if [ ! -d ".venv" ]; then
    echo -e "${RED}✗ Virtual environment not found.${NC}"
    echo -e "${YELLOW}  Run ./scripts/setup_local.sh or 'poetry install' first${NC}"
    exit 1
fi

# Check if node_modules exists
if [ ! -d "code/frontend/node_modules" ]; then
    echo -e "${YELLOW}⚠ Frontend dependencies not installed. Installing...${NC}"
    cd code/frontend && npm install && cd ../..
fi

# Check if local.settings.json exists
if [ ! -f "code/backend/batch/local.settings.json" ]; then
    echo -e "${YELLOW}⚠ local.settings.json not found. Creating default...${NC}"
    cat > code/backend/batch/local.settings.json << 'EOF'
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsStorage": "UseDevelopmentStorage=true"
  },
  "ConnectionStrings": {}
}
EOF
    echo -e "${YELLOW}  Note: Document processing may not work without proper Azure storage config.${NC}"
    echo -e "${YELLOW}  Run ./scripts/setup_local.sh for full Azure integration.${NC}"
fi

echo -e "${GREEN}✓ Setup checks passed${NC}"
echo ""

# Check if PostgreSQL container is running (for TrackMan/Redshift testing)
if ! docker ps | grep -q trackman-postgres; then
    echo -e "${YELLOW}Starting PostgreSQL container (for TrackMan testing)...${NC}"
    docker run -d \
        --name trackman-postgres \
        -e POSTGRES_USER=testuser \
        -e POSTGRES_PASSWORD=testpassword \
        -e POSTGRES_DB=trackman_test \
        -p 5432:5432 \
        postgres:14 2>/dev/null || docker start trackman-postgres 2>/dev/null || true
    echo "Waiting for PostgreSQL to be ready..."
    sleep 3
fi

# Kill any existing processes on our ports
echo -e "${YELLOW}Cleaning up existing processes...${NC}"
lsof -ti:5050 | xargs kill -9 2>/dev/null || true  # Flask backend
lsof -ti:5173 | xargs kill -9 2>/dev/null || true  # Vite frontend
lsof -ti:8501 | xargs kill -9 2>/dev/null || true  # Streamlit Admin UI
lsof -ti:7071 | xargs kill -9 2>/dev/null || true  # Azure Functions

# Export environment variables for TrackMan/Redshift
export USE_REDSHIFT=true
export REDSHIFT_HOST=localhost
export REDSHIFT_PORT=5432
export REDSHIFT_DB=trackman_test
export REDSHIFT_USER=testuser
export REDSHIFT_PASSWORD=testpassword

# Activate virtual environment
source .venv/bin/activate

echo ""
echo -e "${BLUE}--- Starting Chat UI Services ---${NC}"

# Start Flask backend on port 5050
echo -e "${GREEN}Starting Flask backend on http://localhost:5050${NC}"
cd code
nohup flask run --host=127.0.0.1 --port=5050 > /tmp/flask.log 2>&1 &
cd ..

# Wait for Flask to start (needs time to initialize LLMHelper)
sleep 8
if lsof -i:5050 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Flask backend running on port 5050${NC}"
else
    echo -e "${YELLOW}⚠ Flask still starting. Check /tmp/flask.log${NC}"
fi

# Start Vite frontend on port 5173
echo -e "${GREEN}Starting Vite frontend on http://localhost:5173${NC}"
cd code/frontend
nohup npm run dev > /tmp/vite.log 2>&1 &
cd ../..

# Wait for Vite to start
sleep 3
if lsof -i:5173 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Vite frontend running on port 5173${NC}"
else
    echo -e "${RED}✗ Vite failed to start. Check /tmp/vite.log${NC}"
fi

echo ""
echo -e "${BLUE}--- Starting Admin UI ---${NC}"

# Start Streamlit Admin UI on port 8501
echo -e "${GREEN}Starting Streamlit Admin UI on http://localhost:8501${NC}"
cd code/backend
nohup streamlit run Admin.py --server.port 8501 --server.headless true > /tmp/streamlit.log 2>&1 &
cd ../..

# Wait for Streamlit to start
sleep 3
if lsof -i:8501 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Streamlit Admin UI running on port 8501${NC}"
else
    echo -e "${RED}✗ Streamlit failed to start. Check /tmp/streamlit.log${NC}"
fi

echo ""
echo -e "${BLUE}--- Starting Azure Functions ---${NC}"

# Start Azure Functions on port 7071 (for document processing)
echo -e "${GREEN}Starting Azure Functions on http://localhost:7071${NC}"
cd code/backend/batch
nohup func host start --port 7071 > /tmp/functions.log 2>&1 &
cd ../../..

# Wait for Functions to start (needs time to load)
sleep 8
if lsof -i:7071 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Azure Functions running on port 7071${NC}"
else
    echo -e "${YELLOW}⚠ Azure Functions still starting. Check /tmp/functions.log${NC}"
    echo -e "${YELLOW}  (Install with: npm install -g azure-functions-core-tools@4)${NC}"
fi

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║            Local Environment Ready                         ║${NC}"
echo -e "${GREEN}╠════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║${NC} Chat UI (Frontend):  ${BLUE}http://localhost:5173${NC}                ${GREEN}║${NC}"
echo -e "${GREEN}║${NC} Chat API (Backend):  ${BLUE}http://localhost:5050${NC}                ${GREEN}║${NC}"
echo -e "${GREEN}║${NC} Admin UI:            ${BLUE}http://localhost:8501${NC}                ${GREEN}║${NC}"
echo -e "${GREEN}║${NC} Azure Functions:     ${BLUE}http://localhost:7071${NC}                ${GREEN}║${NC}"
echo -e "${GREEN}║${NC} PostgreSQL:          localhost:5432 (trackman_test)     ${GREEN}║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}Logs:${NC}"
echo "  Flask:      tail -f /tmp/flask.log"
echo "  Vite:       tail -f /tmp/vite.log"
echo "  Streamlit:  tail -f /tmp/streamlit.log"
echo "  Functions:  tail -f /tmp/functions.log"
echo ""
echo -e "${YELLOW}To stop all services: ./stop_local.sh${NC}"
echo ""
echo -e "${BLUE}Usage:${NC}"
echo "  • Chat UI: Open http://localhost:5173 to chat with your documents"
echo "  • Admin UI: Open http://localhost:8501 to upload and process documents"
echo "  • TrackMan: Ask database queries like 'Show errors from last 7 days'"
