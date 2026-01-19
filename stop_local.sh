#!/bin/bash
# Stop local development environment

echo "Stopping local development environment..."

# Stop Flask
lsof -ti:5050 | xargs kill -9 2>/dev/null && echo "✓ Flask stopped" || echo "Flask was not running"

# Stop Vite
lsof -ti:5173 | xargs kill -9 2>/dev/null && echo "✓ Vite stopped" || echo "Vite was not running"

echo ""
echo "To also stop PostgreSQL: docker stop trackman-postgres"
echo "Done."
