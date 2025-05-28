#!/bin/bash

# Run all DuckLake demos
# This script executes all 5 demos in sequence

set -e

echo "🦆 DuckLake Demo Suite"
echo "===================="
echo ""
echo "This will run all 5 DuckLake demos:"
echo "  1. Transaction Rollback Safety"
echo "  2. Time Travel Debugging"
echo "  3. Schema Evolution Without Downtime"
echo "  4. Small File Optimization"
echo "  5. Catalog Portability"
echo ""
echo "Press Enter to continue or Ctrl+C to cancel..."
read

# Function to run a demo
run_demo() {
    local demo_num=$1
    local demo_name=$2
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Running Demo $demo_num: $demo_name"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    cd "$demo_num"*
    ./demo.sh
    cd ..
    
    echo ""
    echo "Demo $demo_num completed. Press Enter to continue to the next demo..."
    read
}

# Run all demos
run_demo 01 "Transaction Rollback Safety"
run_demo 02 "Time Travel Debugging"
run_demo 03 "Schema Evolution Without Downtime"
run_demo 04 "Small File Optimization"
run_demo 05 "Catalog Portability"

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🎉 All demos completed successfully!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Key Takeaways:"
echo "  ✅ DuckLake provides true ACID transactions across tables"
echo "  ✅ Time travel makes debugging and recovery simple"
echo "  ✅ Schema changes are transactional with zero downtime"
echo "  ✅ Small updates are efficient without file explosion"
echo "  ✅ Catalog portability enables seamless dev-to-prod workflows"
echo ""
echo "Learn more about DuckLake at: https://duckdb.org/ducklake" 