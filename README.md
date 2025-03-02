# 🦆 DuckTales

## DuckDB ADBC DBAPI Demo

![DuckDB Logo](https://duckdb.org/images/logo-dl/DuckDB_Logo.png)

## 📚 Overview

DuckTales is a comprehensive demonstration of the DuckDB Arrow Database Connectivity (ADBC) DBAPI for Python developers. This project showcases the powerful capabilities of DuckDB when used with the ADBC interface, providing a hands-on guide to leveraging DuckDB's high-performance analytical capabilities through Python.

## ✨ Features

DuckTales demonstrates a wide range of DuckDB ADBC features:

- **Basic Connection & Query Execution**: Learn how to establish connections and execute simple queries
- **Data Ingestion Methods**: Explore multiple ways to load data into DuckDB
- **Query Execution & Result Retrieval**: Discover different methods to execute queries and retrieve results
- **Transaction Management**: Understand how transactions work with the ADBC driver
- **Performance Considerations**: See how DuckDB handles large datasets efficiently
- **Arrow Stream Ingestion**: Learn the most efficient way to ingest data using Arrow streams
- **Advanced SQL Features**: Explore DuckDB's powerful SQL capabilities including:
  - CASE expressions
  - JSON data handling
  - Window functions
  - Pivot operations
- **Error Handling**: Understand how to properly handle errors in your applications

## 🚀 Getting Started

### Prerequisites

- Python 3.7+
- Required packages:

    ```bash
    duckdb
    adbc_driver_manager
    pyarrow
    numpy
    pandas
  ```

### Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/TFMV/ducktales.git
   cd ducktales
   ```

2. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

### Running the Demo

Simply run the main script:

```bash
python app.py
```

The script will execute all demonstrations sequentially, with clear section headers and explanations for each feature.

## 📊 Example Output

The demo provides rich, formatted output for each section:

```bash
================================================================================
===================== Basic Connection and Query Execution =====================
================================================================================

Using global connection...
Connected to: :memory:
Query result as PyArrow table:
pyarrow.Table
answer: int32
----
answer: [[42]]
```

## 🔍 Key Concepts

### In-Memory Database

The demo uses an in-memory database by default, making it fast and easy to run without leaving artifacts on your system:

```python
DB_PATH = ":memory:"  # Use in-memory database instead of file-based
```

### Arrow Integration

DuckDB's tight integration with Apache Arrow is showcased throughout the demo:

```python
# Fetch results as a PyArrow table
tbl = cur.fetch_arrow_table()

# Fetch as Arrow record batch reader
reader = cur.fetch_record_batch()
```

### Batch Processing

Learn how to efficiently process large datasets in batches:

```python
# Process in batches
for batch in reader:
    # Process each batch
    avg_value = pc.mean(batch["value"]).as_py()
```

## 📝 Notes

- The ADBC driver for DuckDB has limited transaction support compared to the native DuckDB Python API
- The `executemany()` method is not supported by the DuckDB ADBC driver
- For production use cases with complex transaction requirements, consider using the native DuckDB Python API


## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [DuckDB](https://duckdb.org/) - The database that makes this all possible
- [Apache Arrow](https://arrow.apache.org/) - For the amazing Arrow data format
- [ADBC](https://arrow.apache.org/docs/format/ADBC.html) - For the Arrow Database Connectivity standard

---

Happy querying! 🦆
