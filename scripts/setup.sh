#!/bin/bash

# DuckLake Demo Setup Script
# This script installs DuckDB and the DuckLake extension

set -e  # Exit on error

echo "🦆 DuckLake Demo Setup"
echo "====================="

# Check if DuckDB is installed
if ! command -v duckdb &> /dev/null; then
    echo "📦 Installing DuckDB..."
    
    # Detect OS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            brew install duckdb
        else
            echo "❌ Homebrew not found. Please install Homebrew or download DuckDB manually."
            echo "   Visit: https://duckdb.org/docs/installation/"
            exit 1
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        echo "📥 Downloading DuckDB for Linux..."
        wget https://github.com/duckdb/duckdb/releases/download/v1.3.0/duckdb_cli-linux-amd64.zip
        unzip duckdb_cli-linux-amd64.zip
        sudo mv duckdb /usr/local/bin/
        rm duckdb_cli-linux-amd64.zip
    else
        echo "❌ Unsupported OS. Please install DuckDB manually."
        echo "   Visit: https://duckdb.org/docs/installation/"
        exit 1
    fi
else
    echo "✅ DuckDB is already installed"
fi

# Verify DuckDB version
echo "📊 DuckDB version:"
duckdb --version

# Create a temporary DuckDB instance to install the extension
echo ""
echo "📦 Installing DuckLake extension..."
cat << 'EOF' | duckdb
INSTALL ducklake;
.quit
EOF

echo ""
echo "✅ Setup complete! DuckDB and DuckLake extension are ready."
echo ""
echo "🚀 To run the demos:"
echo "   cd demos"
echo "   ./run_all_demos.sh"
echo ""
echo "📚 Or run individual demos:"
echo "   cd demos/01_transaction_rollback"
echo "   ./demo.sh" 