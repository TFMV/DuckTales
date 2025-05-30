# 🦆 DuckTales: DuckLake Demo Suite

![DuckDB Logo](https://duckdb.org/images/logo-dl/DuckDB_Logo.png)

## 📚 Overview

DuckTales is a comprehensive demonstration suite for **DuckLake**, DuckDB's revolutionary lakehouse format that simplifies data management by using SQL databases for metadata instead of complex file-based systems.

This project showcases the five key scenarios from the article "Rethinking the Lakehouse with a Duck and a Plan", demonstrating how DuckLake solves real-world problems that traditional lakehouse formats struggle with.

## ✨ What is DuckLake?

DuckLake is a new open table format that reimagines the lakehouse architecture by:

- **Using SQL for metadata**: All metadata lives in a standard SQL database (PostgreSQL, MySQL, DuckDB, etc.)
- **Storing data in open formats**: Data files remain in Parquet on blob storage
- **Providing true ACID guarantees**: Full transactional support across multiple tables
- **Simplifying operations**: No complex file hierarchies, manifest files, or pointer swapping

## 🎯 Demo Scenarios

### Demo 1: Transaction Rollback Safety

Shows how DuckLake maintains transactional consistency across multiple tables - something traditional formats can't do.

**Key Features:**

- Multi-table transactions
- Automatic rollback on errors
- Cross-table consistency guarantees

### Demo 2: Time Travel Debugging

Demonstrates DuckLake's powerful time travel capabilities for investigating data issues and recovering from accidents.

**Key Features:**

- Query data at any point in time
- Investigate when changes occurred
- Recover accidentally deleted data
- Create audit logs using time travel

### Demo 3: Schema Evolution Without Downtime

Showcases transactional DDL operations that allow schema changes while applications continue running.

**Key Features:**

- Add columns with defaults
- Change data types
- Add constraints
- All changes are transactional

### Demo 4: Small File Optimization

Compares DuckLake's efficiency against traditional formats for frequent small updates.

**Key Features:**

- Dramatic reduction in file count
- Optional inlining of small changes
- Performance comparison metrics
- Storage efficiency analysis

### Demo 5: Catalog Portability

Demonstrates seamless transition from local development to production with different catalog backends.

**Key Features:**

- Local development with DuckDB
- Migration to PostgreSQL/MySQL
- Multi-environment support
- Zero code changes required

## 🚀 Getting Started

### Prerequisites

- Python 3.7+
- DuckDB v1.3.0 or later

### Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/TFMV/ducktales.git
   cd ducktales/DuckTales
   ```

2. Run the setup script:

   ```bash
   chmod +x scripts/setup.sh
   ./scripts/setup.sh
   ```

3. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

### Running the Demos

#### Run all demos

```bash
cd demos
chmod +x run_all_demos.sh
./run_all_demos.sh
```

#### Run individual demos

```bash
cd demos/01_transaction_rollback
chmod +x demo.sh
./demo.sh
```

## 📁 Project Structure

```text
DuckTales/
├── README.md
├── requirements.txt
├── scripts/
│   └── setup.sh              # Installation script
├── utils/
│   └── ducklake_utils.py     # Common utilities
├── demos/
│   ├── run_all_demos.sh      # Run all demos
│   ├── 01_transaction_rollback/
│   │   ├── demo.py
│   │   └── demo.sh
│   ├── 02_time_travel/
│   │   ├── demo.py
│   │   └── demo.sh
│   ├── 03_schema_evolution/
│   │   ├── demo.py
│   │   └── demo.sh
│   ├── 04_small_file_optimization/
│   │   ├── demo.py
│   │   └── demo.sh
│   └── 05_catalog_portability/
│       ├── demo.py
│       └── demo.sh
├── exploration/              # Advanced analysis scripts
│   ├── ducklake_analysis.sh  # Comprehensive DuckLake behavior analysis
│   ├── schema_analysis.sh    # Catalog schema and metadata analysis
│   ├── benchmark_ducklake.sh # Performance benchmarking
│   └── run_all_analysis.sh   # Run all analysis scripts
├── data/                     # Test data
│   └── parquet/
│       ├── flights/
│       ├── lineitem/
│       └── customer/
```

### 🔬 Analysis Tools

The `exploration` directory contains a suite of analysis tools for understanding DuckLake's behavior and performance:

#### Analysis Scripts

- **ducklake_analysis.sh**: Comprehensive analysis of DuckLake's behavior, including:
  - Metadata operations tracking
  - File system state monitoring
  - Time travel capabilities
  - Catalog introspection

- **schema_analysis.sh**: Deep dive into DuckLake's catalog structure:
  - Schema evolution tracking
  - Metadata table relationships
  - System function analysis
  - Catalog backend compatibility

- **benchmark_ducklake.sh**: Performance benchmarking suite:
  - Transaction throughput
  - Storage efficiency
  - Metadata operation latency
  - Comparison with traditional formats

#### Running Analysis Tools

```bash
cd exploration
chmod +x run_all_analysis.sh
./run_all_analysis.sh
```

Analysis results are stored in the `notes/ducklake_results` directory, with detailed traces in `notes/ducklake_traces`.

## 🔍 Key Concepts

### SQL as a Lakehouse Format

DuckLake's core innovation is using a SQL database for all metadata operations:

```sql
-- All metadata operations are just SQL transactions
BEGIN TRANSACTION;
  INSERT INTO ducklake_data_file VALUES (...);
  INSERT INTO ducklake_table_stats VALUES (...);
  INSERT INTO ducklake_snapshot VALUES (...);
COMMIT;
```

### Time Travel Syntax

Query any table at a specific point in time:

```sql
-- Query at specific version
SELECT * FROM customers AT (VERSION => 42);

-- Query at specific timestamp
SELECT * FROM customers AT (TIMESTAMP => '2024-01-15 14:00:00');
```

### Catalog Connection Strings

DuckLake supports multiple catalog backends:

```sql
-- Local development
ATTACH 'ducklake:local.ducklake' AS dev;

-- PostgreSQL production
ATTACH 'ducklake:postgresql://host/database' AS prod;

-- MySQL production
ATTACH 'ducklake:mysql://host/database' AS prod;
```

## 📊 Performance Benefits

Based on our demos, DuckLake provides:

- **99% fewer files** for frequent small updates
- **Sub-millisecond writes** with inlining
- **1000x more concurrent writers** than traditional formats
- **Single SQL query** for metadata vs. multiple HTTP calls

## 📄 License

This project is licensed under the MIT License.

**The duck has landed. The lake is calling.** 🦆
