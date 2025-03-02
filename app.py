#!/usr/bin/env python3
"""
DuckDB ADBC DBAPI Demo

This script demonstrates the features and capabilities of the DuckDB ADBC DBAPI
for Python developers. It showcases various ways to interact with DuckDB through
the Arrow Database Connectivity (ADBC) interface.
"""

import adbc_driver_duckdb.dbapi
import pyarrow
import pyarrow.compute as pc
import numpy as np
import pandas as pd
import os
import time
from typing import List, Dict, Any, Optional, Tuple

# Constants
DB_PATH = ":memory:"  # Use in-memory database instead of file-based
BATCH_SIZE = 10000


def print_section(title: str) -> None:
    """Print a section title with formatting."""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80 + "\n")


def cleanup_db() -> None:
    """Clean up function - not needed for in-memory database but kept for compatibility."""
    print("Using in-memory database, no cleanup needed.")


def basic_connection_demo() -> None:
    """Demonstrate basic connection and simple query execution."""
    print_section("Basic Connection and Query Execution")

    # Use the global connection
    print("Using global connection...")
    print(f"Connected to: {DB_PATH}")

    # Create a cursor
    with conn.cursor() as cur:
        # Execute a simple query
        cur.execute("SELECT 42 AS answer")

        # Fetch results as a PyArrow table
        tbl = cur.fetch_arrow_table()
        print("Query result as PyArrow table:")
        print(tbl)

        # Fetch results using standard DB-API methods
        cur.execute("SELECT 'Hello' AS greeting, 'World' AS target")

        # Get column descriptions
        print("\nColumn descriptions:")
        for col in cur.description:
            print(f"  {col[0]}: {col[1]}")

        # Fetch one row
        print("\nFetching one row:")
        print(cur.fetchone())

        # Execute another query and fetch all rows
        cur.execute("SELECT * FROM range(1, 5) AS t(num)")
        print("\nFetching all rows:")
        print(cur.fetchall())


def data_ingestion_demo() -> None:
    """Demonstrate different ways to ingest data into DuckDB."""
    print_section("Data Ingestion Methods")

    # 1. Ingest data from PyArrow record batch
    print("1. Ingesting data from PyArrow record batch")
    data = pyarrow.record_batch(
        [[1, 2, 3, 4], ["a", "b", "c", "d"]],
        names=["id", "letter"],
    )

    # Use the global connection
    with conn.cursor() as cur:
        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS simple_data")

        # Ingest data using adbc_ingest
        cur.adbc_ingest("simple_data", data)

        # Verify the data was ingested
        cur.execute("SELECT * FROM simple_data ORDER BY id")
        result = cur.fetch_arrow_table()
        print(result)

    # 2. Ingest data using SQL INSERT with parameters
    print("\n2. Ingesting data using SQL INSERT with parameters")
    # Use the global connection
    with conn.cursor() as cur:
        # Create a new table
        cur.execute("DROP TABLE IF EXISTS param_data")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS param_data (
                id INTEGER,
                name VARCHAR,
                value DOUBLE
            )
        """
        )

        # Insert single row with parameters
        cur.execute("INSERT INTO param_data VALUES (?, ?, ?)", (1, "alpha", 100.5))

        # Insert multiple rows with individual execute calls
        # Note: executemany is not supported by the DuckDB ADBC driver
        params = [
            (2, "beta", 200.5),
            (3, "gamma", 300.5),
            (4, "delta", 400.5),
        ]
        print("Inserting multiple rows with individual execute calls...")
        for param in params:
            cur.execute("INSERT INTO param_data VALUES (?, ?, ?)", param)

        # Verify the data was ingested
        cur.execute("SELECT * FROM param_data ORDER BY id")
        result = cur.fetch_arrow_table()
        print(result)


def query_execution_demo() -> None:
    """Demonstrate different query execution methods and result retrieval."""
    print_section("Query Execution and Result Retrieval")

    # Use the global connection
    with conn.cursor() as cur:
        # 1. Basic query execution
        print("1. Basic query execution")
        cur.execute(
            """
            SELECT 
                id, 
                letter
            FROM simple_data
            WHERE id > 1
            ORDER BY id
        """
        )

        # Fetch as PyArrow table
        result = cur.fetch_arrow_table()
        print("Result as PyArrow table:")
        print(result)

        # 2. Query with parameters
        print("\n2. Query with parameters")
        cur.execute("SELECT * FROM param_data WHERE id > ? AND value < ?", (2, 400.0))

        # Fetch as list of tuples
        rows = cur.fetchall()
        print("Result as list of tuples:")
        for row in rows:
            print(row)

        # 3. Get result metadata
        print("\n3. Result metadata")
        cur.execute("SELECT * FROM simple_data LIMIT 1")

        # Print column descriptions
        print("Column descriptions:")
        for col in cur.description:
            name, type_code, display_size, internal_size, precision, scale, null_ok = (
                col
            )
            print(f"  {name}: type={type_code}, null_ok={null_ok}")

        # 4. Get row count
        print("\n4. Row count")
        cur.execute("SELECT * FROM simple_data")
        print(f"Row count: {cur.rowcount}")

        # 5. Fetch as Arrow record batch reader
        print("\n5. Fetch as Arrow record batch reader")
        cur.execute("SELECT * FROM simple_data ORDER BY id")
        reader = cur.fetch_record_batch()

        # Process record batches
        print("Processing record batches:")
        for i, batch in enumerate(reader):
            print(f"  Batch {i+1}: {len(batch)} rows")
            print(batch.to_pandas())


def transaction_demo() -> None:
    """Demonstrate transaction management."""
    print_section("Transaction Management")

    # Use the global connection
    # Note: The ADBC driver for DuckDB doesn't support explicit transaction control
    # through begin(), commit(), and rollback() methods on the connection object.
    # Instead, it uses autocommit mode by default.

    print("Note: The ADBC driver for DuckDB has limited transaction support.")
    print("We'll demonstrate what's available with the current driver.")

    with conn.cursor() as cur:
        # 1. Basic transaction with autocommit
        print("\n1. Basic operations with autocommit")

        # Create a new table
        cur.execute("DROP TABLE IF EXISTS transaction_test")
        cur.execute("CREATE TABLE transaction_test (id INTEGER, value VARCHAR)")

        # Insert data
        cur.execute("INSERT INTO transaction_test VALUES (1, 'auto-committed')")

        # Verify the data was inserted
        cur.execute("SELECT * FROM transaction_test ORDER BY id")
        print("Data after insert:")
        print(cur.fetch_arrow_table())

        # 2. Multiple operations
        print("\n2. Multiple operations")

        # Multiple operations
        cur.execute("INSERT INTO transaction_test VALUES (2, 'second row')")
        cur.execute("INSERT INTO transaction_test VALUES (3, 'third row')")
        cur.execute("UPDATE transaction_test SET value = 'updated' WHERE id = 1")

        # Verify the changes
        cur.execute("SELECT * FROM transaction_test ORDER BY id")
        print("Final table state:")
        print(cur.fetch_arrow_table())

        # 3. Explain transaction behavior
        print("\n3. Transaction behavior in DuckDB ADBC")
        print("The DuckDB ADBC driver operates in autocommit mode by default.")
        print(
            "For full transaction control, consider using the native DuckDB Python API."
        )


def performance_demo() -> None:
    """Demonstrate performance considerations and batch processing."""
    print_section("Performance Considerations")

    # Create a large dataset
    print("Creating a large dataset...")
    num_rows = 100000

    # Create data with numpy for efficiency
    np.random.seed(42)
    ids = np.arange(num_rows)
    values = np.random.random(num_rows)
    categories = np.random.choice(["A", "B", "C", "D", "E"], num_rows)

    # Convert to PyArrow
    table = pyarrow.Table.from_arrays(
        [ids, values, categories], names=["id", "value", "category"]
    )

    # Time the ingestion
    print(f"Ingesting {num_rows} rows...")
    start_time = time.time()

    # Use the global connection
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS performance_test")
        cur.adbc_ingest("performance_test", table)

    elapsed = time.time() - start_time
    print(f"Ingestion completed in {elapsed:.2f} seconds")

    # Demonstrate batch processing
    print("\nDemonstrating batch processing with record batch reader...")
    # Use the global connection
    with conn.cursor() as cur:
        # Execute a query that returns a lot of data
        cur.execute("SELECT * FROM performance_test ORDER BY id")

        # Get a record batch reader
        reader = cur.fetch_record_batch()

        # Process in batches
        batch_count = 0
        total_rows = 0

        start_time = time.time()
        for batch in reader:
            batch_count += 1
            total_rows += len(batch)

            # Here you would process each batch
            # For demonstration, we'll just compute some aggregates
            if batch_count <= 3:  # Only show details for first few batches
                avg_value = pc.mean(batch["value"]).as_py()
                print(
                    f"Batch {batch_count}: {len(batch)} rows, avg value: {avg_value:.4f}"
                )

        elapsed = time.time() - start_time
        print(
            f"Processed {batch_count} batches ({total_rows} rows) in {elapsed:.2f} seconds"
        )


def arrow_stream_demo() -> None:
    """Demonstrate data ingestion using Arrow streams."""
    print_section("Arrow Stream Ingestion")

    print(
        "This demo shows how to ingest data using Arrow streams, which is more efficient for large datasets"
    )

    # Create multiple record batches to simulate a stream
    print("Creating record batches for streaming...")

    # Create 5 batches with different data
    batches = []
    np.random.seed(42)

    for batch_id in range(5):
        # Create batch with 1000 rows each
        start_id = batch_id * 1000
        ids = np.arange(start_id, start_id + 1000)
        temps = np.random.normal(20, 5, 1000)  # Temperature data with mean 20, std 5
        cities = np.random.choice(
            ["New York", "London", "Tokyo", "Sydney", "Berlin"], 1000
        )
        timestamps = [
            pd.Timestamp("2023-01-01") + pd.Timedelta(hours=i) for i in range(1000)
        ]

        # Create a record batch
        batch = pyarrow.record_batch(
            [
                pyarrow.array(ids),
                pyarrow.array(temps),
                pyarrow.array(cities),
                pyarrow.array(timestamps),
            ],
            names=["id", "temperature", "city", "timestamp"],
        )

        batches.append(batch)

    # Create a record batch reader from the batches
    reader = pyarrow.RecordBatchReader.from_batches(batches[0].schema, batches)

    print("Ingesting data from Arrow stream...")
    start_time = time.time()

    # Use the global connection
    with conn.cursor() as cur:
        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS weather_data")

        # Ingest data using the stream
        # Method 1: Using adbc_ingest with a reader
        cur.adbc_ingest("weather_data", reader)

        # Verify the data was ingested
        cur.execute("SELECT COUNT(*) AS row_count FROM weather_data")
        print("Total rows ingested:", cur.fetch_arrow_table())

        # Show sample of the data
        cur.execute(
            """
            SELECT 
                city, 
                MIN(temperature) AS min_temp,
                AVG(temperature) AS avg_temp,
                MAX(temperature) AS max_temp,
                COUNT(*) AS readings
            FROM weather_data
            GROUP BY city
            ORDER BY avg_temp DESC
        """
        )
        print("\nTemperature summary by city:")
        print(cur.fetch_arrow_table())

    elapsed = time.time() - start_time
    print(f"Stream ingestion completed in {elapsed:.2f} seconds")

    # Demonstrate another way to ingest with Arrow streams
    print("\nDemonstrating alternative stream ingestion method...")

    # Create a new table with different schema
    table = pyarrow.table(
        {
            "product_id": np.arange(1000),
            "product_name": [f"Product-{i}" for i in range(1000)],
            "price": np.random.uniform(10, 1000, 1000),
            "in_stock": np.random.choice([True, False], 1000),
        }
    )

    # Convert to a stream
    batches = []
    for batch in table.to_batches(max_chunksize=200):  # Split into batches of 200 rows
        batches.append(batch)

    reader = pyarrow.RecordBatchReader.from_batches(batches[0].schema, batches)

    # Use the global connection
    with conn.cursor() as cur:
        # Drop table if exists
        cur.execute("DROP TABLE IF EXISTS products")

        # Method 2: Using adbc_ingest with a reader
        cur.adbc_ingest("products", reader)

        # Verify the data was ingested
        cur.execute(
            """
            SELECT 
                COUNT(*) AS total_products,
                SUM(CASE WHEN in_stock THEN 1 ELSE 0 END) AS in_stock_count,
                AVG(price) AS avg_price
            FROM products
        """
        )
        print("Product summary:")
        print(cur.fetch_arrow_table())


def advanced_features_demo() -> None:
    """Demonstrate advanced features of DuckDB through ADBC."""
    print_section("Advanced Features")

    # Use the global connection
    with conn.cursor() as cur:
        # 1. Advanced SQL features
        print("1. Advanced SQL features")

        # Create a sample table
        cur.execute("DROP TABLE IF EXISTS advanced_test")
        cur.execute(
            """
            CREATE TABLE advanced_test AS
            SELECT * FROM (
                VALUES
                (1, 'apple', 10.5, '2023-01-01'),
                (2, 'banana', 20.5, '2023-01-02'),
                (3, 'cherry', 30.5, '2023-01-03'),
                (4, 'date', 40.5, '2023-01-04'),
                (5, 'elderberry', 50.5, '2023-01-05')
            ) AS t(id, name, value, date);
            """
        )

        # Demonstrate CASE expression
        cur.execute(
            """
            SELECT 
                id, 
                name,
                CASE 
                    WHEN value < 20 THEN 'low'
                    WHEN value < 40 THEN 'medium'
                    ELSE 'high'
                END AS category
            FROM advanced_test
            ORDER BY id
            """
        )
        print("CASE expression example:")
        print(cur.fetch_arrow_table())

        # 2. Working with JSON
        print("\n2. Working with JSON data")

        # Create a table with JSON data
        cur.execute("DROP TABLE IF EXISTS json_test")
        cur.execute(
            """
            CREATE TABLE json_test AS
            SELECT * FROM (
                VALUES
                ('{"name": "Alice", "age": 30, "hobbies": ["reading", "hiking"]}'),
                ('{"name": "Bob", "age": 25, "hobbies": ["gaming", "cooking"]}'),
                ('{"name": "Charlie", "age": 35, "hobbies": ["swimming", "photography"]}')
            ) AS t(json_data);
        """
        )

        # Query JSON data using correct DuckDB JSON functions
        cur.execute(
            """
            SELECT 
                json_extract_string(json_data, '$.name') AS name,
                CAST(json_extract_string(json_data, '$.age') AS INTEGER) AS age,
                json_extract(json_data, '$.hobbies') AS hobbies
            FROM json_test
            ORDER BY name
        """
        )
        print(cur.fetch_arrow_table())

        # 3. Window functions
        print("\n3. Window functions")

        # Create sample data
        cur.execute("DROP TABLE IF EXISTS sales")
        cur.execute(
            """
            CREATE TABLE sales AS
            SELECT * FROM (
                VALUES
                ('2023-01-01', 'East', 100),
                ('2023-01-02', 'East', 150),
                ('2023-01-03', 'East', 200),
                ('2023-01-01', 'West', 50),
                ('2023-01-02', 'West', 75),
                ('2023-01-03', 'West', 80)
            ) AS t(date, region, amount);
        """
        )

        # Use window functions
        cur.execute(
            """
            SELECT 
                date, 
                region, 
                amount,
                SUM(amount) OVER (PARTITION BY region ORDER BY date) AS running_total,
                amount / SUM(amount) OVER (PARTITION BY region) AS pct_of_region
            FROM sales
            ORDER BY region, date
        """
        )
        print(cur.fetch_arrow_table())

        # 4. Pivot operations
        print("\n4. Manual pivot implementation")

        # Use a manual pivot implementation with conditional aggregation
        cur.execute(
            """
            SELECT 
                date,
                SUM(CASE WHEN region = 'East' THEN amount ELSE 0 END) AS East,
                SUM(CASE WHEN region = 'West' THEN amount ELSE 0 END) AS West
            FROM sales
            GROUP BY date
            ORDER BY date
            """
        )
        print(cur.fetch_arrow_table())


def error_handling_demo() -> None:
    """Demonstrate error handling with ADBC."""
    print_section("Error Handling")

    # Use the global connection
    with conn.cursor() as cur:
        # 1. Syntax error
        print("1. Handling syntax errors")
        try:
            cur.execute("SELCT * FROM non_existent_table")
        except Exception as e:
            print(f"Caught syntax error: {e}")

        # 2. Table not found
        print("\n2. Handling table not found errors")
        try:
            cur.execute("SELECT * FROM non_existent_table")
        except Exception as e:
            print(f"Caught table not found error: {e}")

        # 3. Data type mismatch
        print("\n3. Handling data type mismatch errors")
        try:
            cur.execute("SELECT 'text' + 42")
        except Exception as e:
            print(f"Caught type mismatch error: {e}")

        # 4. Constraint violation
        print("\n4. Handling constraint violations")
        try:
            # Create a table with a primary key
            cur.execute("DROP TABLE IF EXISTS pk_test")
            cur.execute("CREATE TABLE pk_test (id INTEGER PRIMARY KEY, value TEXT)")
            cur.execute("INSERT INTO pk_test VALUES (1, 'first')")

            # Try to insert a duplicate key
            cur.execute("INSERT INTO pk_test VALUES (1, 'duplicate')")
        except Exception as e:
            print(f"Caught constraint violation: {e}")


def cleanup_and_close() -> None:
    """Perform final cleanup operations."""
    print_section("Cleanup and Close")

    # Note: We're not closing the global connection here as it's done in main()
    print(
        "Cleanup complete. The global connection will be closed at the end of the demo."
    )


def main() -> None:
    """Main function to run the demo."""
    print_section("DuckDB ADBC DBAPI Demo")
    print("This script demonstrates the features of the DuckDB ADBC DBAPI for Python.")

    # No need to clean up for in-memory database
    cleanup_db()

    # Create a single connection for the entire demo
    # With in-memory database, we need to keep the connection open for all demos
    print("Setting up database tables...")

    # Create a global connection that will be used by all demos
    global conn

    # Close any existing connection first to ensure a fresh start
    if "conn" in globals():
        try:
            conn.close()
        except:
            pass

    # Create a new connection
    conn = adbc_driver_duckdb.dbapi.connect(DB_PATH)

    try:
        with conn.cursor() as cur:
            # Create simple_data table
            print("Creating simple_data table...")
            cur.execute("DROP TABLE IF EXISTS simple_data")
            data = pyarrow.record_batch(
                [[1, 2, 3, 4], ["a", "b", "c", "d"]],
                names=["id", "letter"],
            )
            cur.adbc_ingest("simple_data", data)

            # Create param_data table
            print("Creating param_data table...")
            cur.execute("DROP TABLE IF EXISTS param_data")
            cur.execute(
                """
                CREATE TABLE param_data (
                    id INTEGER,
                    name VARCHAR,
                    value DOUBLE
                )
            """
            )
            cur.execute("INSERT INTO param_data VALUES (1, 'alpha', 100.5)")
            params = [
                (2, "beta", 200.5),
                (3, "gamma", 300.5),
                (4, "delta", 400.5),
            ]
            for param in params:
                cur.execute("INSERT INTO param_data VALUES (?, ?, ?)", param)

        print("Database setup complete. Running demos...")
    except Exception as e:
        print(f"Error during database setup: {e}")
        print("Attempting to continue with demos...")

    # Modify all demo functions to use the global connection
    # We'll need to update the connection handling in each demo function

    # Run the demos
    basic_connection_demo()
    # Skip data_ingestion_demo since we've already created the tables
    # data_ingestion_demo()  # Commented out to avoid duplicate table creation
    query_execution_demo()
    transaction_demo()
    performance_demo()
    arrow_stream_demo()
    advanced_features_demo()
    error_handling_demo()
    cleanup_and_close()

    # Close the global connection at the end
    if "conn" in globals():
        conn.close()
        print("Global connection closed.")

    print("\nDemo completed!")


if __name__ == "__main__":
    main()
