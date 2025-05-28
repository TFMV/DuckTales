#!/bin/bash

# Demo 5: Catalog Portability
# Wrapper script to run the Python demo

set -e

echo "🦆 DuckLake Demo 5: Catalog Portability"
echo "======================================"
echo ""

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
    echo "❌ DuckDB is not installed. Please run: ../../scripts/setup.sh"
    exit 1
fi

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed."
    exit 1
fi

# Check for psycopg2 (optional for PostgreSQL demo)
if ! python3 -c "import psycopg2" 2>/dev/null; then
    echo "⚠️  psycopg2 not installed. PostgreSQL demo will be simulated."
    echo "   To enable PostgreSQL demo: pip install psycopg2-binary"
    echo ""
fi

# Run the demo
python3 demo.py

echo ""
echo "🎉 Demo completed successfully!" 