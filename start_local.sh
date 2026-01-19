#!/bin/bash
# Local development startup script for TrackMan integration testing
# This script starts both the Flask backend and Vite frontend with correct configuration

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== TrackMan Local Development Environment ===${NC}"

# Check if PostgreSQL container is running
if ! docker ps | grep -q trackman-postgres; then
    echo -e "${YELLOW}Starting PostgreSQL container...${NC}"
    docker run -d \
        --name trackman-postgres \
        -e POSTGRES_USER=testuser \
        -e POSTGRES_PASSWORD=testpassword \
        -e POSTGRES_DB=trackman_test \
        -p 5432:5432 \
        postgres:14 2>/dev/null || docker start trackman-postgres
    echo "Waiting for PostgreSQL to be ready..."
    sleep 3
fi

# Kill any existing Flask/Vite processes on our ports
echo -e "${YELLOW}Cleaning up existing processes...${NC}"
lsof -ti:5050 | xargs kill -9 2>/dev/null || true
lsof -ti:5173 | xargs kill -9 2>/dev/null || true

# Export environment variables for TrackMan/Redshift
export USE_REDSHIFT=true
export REDSHIFT_HOST=localhost
export REDSHIFT_PORT=5432
export REDSHIFT_DB=trackman_test
export REDSHIFT_USER=testuser
export REDSHIFT_PASSWORD=testpassword

# Activate virtual environment
source .venv/bin/activate

# Start Flask backend on port 5050
echo -e "${GREEN}Starting Flask backend on http://localhost:5050${NC}"
cd code
nohup flask run --host=127.0.0.1 --port=5050 > /tmp/flask.log 2>&1 &
FLASK_PID=$!
cd ..

# Wait for Flask to start
sleep 2
if lsof -i:5050 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Flask backend running on port 5050${NC}"
else
    echo -e "${RED}✗ Flask failed to start. Check /tmp/flask.log${NC}"
    exit 1
fi

# Start Vite frontend on port 5173
echo -e "${GREEN}Starting Vite frontend on http://localhost:5173${NC}"
cd code/frontend
npm run dev > /tmp/vite.log 2>&1 &
VITE_PID=$!
cd ../..

# Wait for Vite to start
sleep 3
if lsof -i:5173 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Vite frontend running on port 5173${NC}"
else
    echo -e "${RED}✗ Vite failed to start. Check /tmp/vite.log${NC}"
fi

echo ""
echo -e "${GREEN}=== Local Environment Ready ===${NC}"
echo -e "Frontend: ${GREEN}http://localhost:5173${NC}"
echo -e "Backend:  ${GREEN}http://localhost:5050${NC}"
echo -e "Database: PostgreSQL @ localhost:5432 (trackman_test)"
echo ""
echo -e "${YELLOW}Logs:${NC}"
echo "  Flask: tail -f /tmp/flask.log"
echo "  Vite:  tail -f /tmp/vite.log"
echo ""
echo -e "${YELLOW}To stop: ./stop_local.sh${NC}"
echo ""
echo "Try these queries in the frontend:"
echo "  • Show me errors from the last 7 days"
echo "  • Which facilities have the most disconnections?"
echo "  • Show error summary for Facility 3218"
