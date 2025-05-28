#!/bin/bash

# Demo 4: Small File Optimization
# Wrapper script to run the Python demo

set -e

echo "🦆 DuckLake Demo 4: Small File Optimization"
echo "==========================================="
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

# Run the demo
python3 demo.py

echo ""
echo "🎉 Demo completed successfully!" 