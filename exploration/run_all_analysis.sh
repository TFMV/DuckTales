#!/bin/bash

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

echo -e "${PURPLE}╔════════════════════════════════════════════╗${NC}"
echo -e "${PURPLE}║     DuckLake vs Iceberg Analysis Suite    ║${NC}"
echo -e "${PURPLE}║            Comprehensive Testing          ║${NC}"
echo -e "${PURPLE}╚════════════════════════════════════════════╝${NC}"
echo ""

# Check prerequisites
echo -e "${BLUE}=== Checking Prerequisites ===${NC}"

if ! command -v duckdb &> /dev/null; then
    echo -e "${RED}Error: DuckDB not found. Please install DuckDB first.${NC}"
    exit 1
fi

if ! command -v bc &> /dev/null; then
    echo -e "${YELLOW}Warning: bc not found. Timing precision may be reduced.${NC}"
fi

DUCKDB_VERSION=$(duckdb --version)
echo -e "${GREEN}✓ DuckDB found: $DUCKDB_VERSION${NC}"

# Create master results directory
MASTER_DIR="../notes/complete_analysis_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$MASTER_DIR"

echo -e "${GREEN}✓ Results will be saved to: $MASTER_DIR${NC}"
echo ""

# Function to run script and capture results
run_analysis() {
    local script_name="$1"
    local description="$2"
    
    echo -e "${YELLOW}=== Running: $description ===${NC}"
    
    if [ ! -f "$script_name" ]; then
        echo -e "${RED}Error: Script $script_name not found${NC}"
        return 1
    fi
    
    # Make script executable
    chmod +x "$script_name"
    
    local start_time=$(date +%s)
    
    # Run the script and capture output
    if ./"$script_name" > "$MASTER_DIR/${script_name%.*}_output.log" 2>&1; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo -e "${GREEN}✓ $description completed in ${duration}s${NC}"
        echo "$script_name,$description,$duration,SUCCESS" >> "$MASTER_DIR/execution_summary.csv"
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        echo -e "${RED}✗ $description failed after ${duration}s${NC}"
        echo "$script_name,$description,$duration,FAILED" >> "$MASTER_DIR/execution_summary.csv"
        return 1
    fi
}

# Initialize execution summary
echo "Script,Description,Duration_Seconds,Status" > "$MASTER_DIR/execution_summary.csv"

# Run all analysis scripts
echo -e "${BLUE}Starting comprehensive analysis...${NC}"
echo ""

# 1. Schema Analysis
run_analysis "schema_analysis.sh" "Schema & Catalog Structure Analysis"

# 2. Main DuckLake Analysis  
run_analysis "ducklake_analysis.sh" "Comprehensive DuckLake Operations"

# 3. Performance Benchmarking
run_analysis "benchmark_ducklake.sh" "Performance Benchmarking"

echo ""
echo -e "${BLUE}=== Consolidating Results ===${NC}"

# Copy all generated notes to master directory
echo -e "${YELLOW}Collecting analysis results...${NC}"

# Copy results from individual analysis runs
for notes_dir in ../notes/ducklake_*; do
    if [ -d "$notes_dir" ]; then
        dir_name=$(basename "$notes_dir")
        cp -r "$notes_dir" "$MASTER_DIR/$dir_name" 2>/dev/null || echo "Note: Could not copy $notes_dir"
    fi
done

# Generate master summary report
echo -e "${YELLOW}Generating master summary...${NC}"

cat > "$MASTER_DIR/MASTER_REPORT.md" << EOF
# DuckLake Analysis - Complete Report

**Analysis Date:** $(date)
**DuckDB Version:** $DUCKDB_VERSION
**System:** $(uname -a)

## Executive Summary

This comprehensive analysis examines DuckLake's architecture, performance characteristics, and metadata management approach in preparation for a detailed comparison with Apache Iceberg.

## Analysis Components

### 1. Schema & Catalog Analysis
- **Purpose:** Understand DuckLake's SQL-based catalog structure
- **Key Findings:** [See ducklake_schema/ directory]

### 2. Operational Analysis  
- **Purpose:** Examine CREATE, INSERT, UPDATE, DELETE operations
- **Key Findings:** [See ducklake_results/ directory]

### 3. Performance Benchmarking
- **Purpose:** Measure timing and resource usage across operations
- **Key Findings:** [See ducklake_performance/ directory]

## Execution Summary

| Script | Description | Duration | Status |
|--------|-------------|----------|--------|
EOF

# Add execution results to summary
while IFS=',' read -r script description duration status; do
    if [[ "$script" != "Script" ]]; then  # Skip header
        printf "| %-20s | %-35s | %8s | %-7s |\n" "$script" "$description" "${duration}s" "$status" >> "$MASTER_DIR/MASTER_REPORT.md"
    fi
done < "$MASTER_DIR/execution_summary.csv"

cat >> "$MASTER_DIR/MASTER_REPORT.md" << 'EOF'

## Directory Structure

```
complete_analysis_YYYYMMDD_HHMMSS/
├── MASTER_REPORT.md                 # This summary
├── execution_summary.csv            # Script execution timing
├── *_output.log                     # Individual script outputs
├── ducklake_schema/                 # Schema analysis results
│   ├── ducklake_schema_summary.md
│   ├── filesystem_structure.txt
│   └── analysis_output.txt
├── ducklake_results/                # Operational analysis
│   ├── analysis_summary.md
│   ├── fs_state_*.txt
│   └── main_analysis_*.out
└── ducklake_performance/            # Performance benchmarks
    ├── performance_summary.md
    ├── benchmark_results.csv
    └── *_output.log
```

## Next Steps for Article

1. **Compare with Iceberg:** Use iceberg_deep_dive.md as baseline
2. **Metadata Complexity:** DuckLake's SQL vs Iceberg's file-based approach
3. **Performance Trade-offs:** Single-node ACID vs distributed coordination
4. **Use Case Analysis:** When to choose each approach

## Key Insights for Article

### DuckLake Strengths
- **Simple Architecture:** Self-contained SQL database
- **Fast Metadata:** SQL queries vs file parsing
- **ACID Guarantees:** Traditional database consistency
- **Familiar Interface:** Standard SQL operations

### DuckLake Considerations  
- **Scalability:** Single database vs distributed metadata
- **Ecosystem:** Emerging vs mature (Iceberg)
- **Cloud Integration:** Simple but less cloud-native
- **Catalog Sharing:** Database-centric vs service-oriented

EOF

# Calculate total analysis time
TOTAL_DURATION=$(awk -F',' 'NR>1 {sum+=$3} END {print sum}' "$MASTER_DIR/execution_summary.csv")

cat >> "$MASTER_DIR/MASTER_REPORT.md" << EOF

**Total Analysis Time:** ${TOTAL_DURATION} seconds

---
*Generated by DuckLake Analysis Suite*
EOF

echo ""
echo -e "${GREEN}=== Analysis Complete ===${NC}"
echo -e "📊 ${YELLOW}Master Report:${NC} $MASTER_DIR/MASTER_REPORT.md"
echo -e "📁 ${YELLOW}All Results:${NC} $MASTER_DIR/"
echo ""

# Display quick summary
echo -e "${BLUE}=== Quick Summary ===${NC}"
printf "%-25s %15s %10s\n" "Analysis Component" "Duration (sec)" "Status"
printf "%-25s %15s %10s\n" "-------------------------" "---------------" "----------"

while IFS=',' read -r script description duration status; do
    if [[ "$script" != "Script" ]]; then
        # Extract short name
        short_name=$(echo "$description" | cut -d' ' -f1-2)
        printf "%-25s %15s %10s\n" "$short_name" "$duration" "$status"
    fi
done < "$MASTER_DIR/execution_summary.csv"

echo ""
echo -e "${PURPLE}🎯 Ready for article writing! Use results in:${NC}"
echo -e "   ${YELLOW}$MASTER_DIR/${NC}"

# Cleanup any temporary files in current directory
echo -e "${YELLOW}Cleaning up temporary files...${NC}"
rm -f *.db *.ducklake *.sql bench_*.sql 2>/dev/null || true

echo -e "${GREEN}✨ All done!${NC}" 