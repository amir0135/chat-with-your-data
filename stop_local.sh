#!/bin/bash
# Stop local development environment - all services

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Stopping local development environment...${NC}"
echo ""

# Stop Flask (Chat API)
lsof -ti:5050 | xargs kill -9 2>/dev/null && echo -e "${GREEN}✓${NC} Flask backend stopped" || echo "  Flask was not running"

# Stop Vite (Chat Frontend)
lsof -ti:5173 | xargs kill -9 2>/dev/null && echo -e "${GREEN}✓${NC} Vite frontend stopped" || echo "  Vite was not running"

# Stop Streamlit (Admin UI)
lsof -ti:8501 | xargs kill -9 2>/dev/null && echo -e "${GREEN}✓${NC} Streamlit Admin UI stopped" || echo "  Streamlit was not running"

# Stop Azure Functions
lsof -ti:7071 | xargs kill -9 2>/dev/null && echo -e "${GREEN}✓${NC} Azure Functions stopped" || echo "  Azure Functions was not running"

echo ""
echo -e "${YELLOW}Optional cleanup:${NC}"
echo "  Stop PostgreSQL:  docker stop trackman-postgres"
echo "  Remove container: docker rm trackman-postgres"
echo ""
echo "Done."
