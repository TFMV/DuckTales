#!/usr/bin/env python3
"""
DuckLake Utility Functions

Common utilities for working with DuckLake in the demos.
"""

import duckdb
import os
import time
from typing import Optional, List, Dict, Any
from datetime import datetime


class DuckLakeConnection:
    """Context manager for DuckLake connections."""

    def __init__(
        self, catalog_path: str = "ducklake:demo.ducklake", database_name: str = "lake"
    ):
        self.catalog_path = catalog_path
        self.database_name = database_name
        self.conn = None

    def __enter__(self):
        self.conn = duckdb.connect()
        self.conn.execute(f"ATTACH '{self.catalog_path}' AS {self.database_name}")
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()


def print_section(title: str, width: int = 80) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * width)
    print(f" {title} ".center(width))
    print("=" * width + "\n")


def print_query_result(
    conn: duckdb.DuckDBPyConnection, query: str, title: Optional[str] = None
) -> None:
    """Execute a query and print the results in a formatted way."""
    if title:
        print(f"\n📊 {title}")
        print("-" * 60)

    print(f"Query: {query}")
    print()

    result = conn.execute(query).fetchdf()
    print(result.to_string())
    print()


def show_snapshots(conn: duckdb.DuckDBPyConnection, database: str = "lake") -> None:
    """Display all snapshots for the current database."""
    print_query_result(
        conn, f"SELECT * FROM ducklake_snapshots('{database}')", "Available Snapshots"
    )


def show_table_changes(
    conn: duckdb.DuckDBPyConnection,
    database: str,
    schema: str,
    table: str,
    start_version: int,
    end_version: int,
) -> None:
    """Display changes between two versions of a table."""
    print_query_result(
        conn,
        f"SELECT * FROM ducklake_table_changes('{database}', '{schema}', '{table}', {start_version}, {end_version})",
        f"Changes from version {start_version} to {end_version}",
    )


def time_operation(operation_name: str):
    """Decorator to time operations."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            print(f"⏱️  {operation_name} completed in {elapsed:.3f} seconds")
            return result

        return wrapper

    return decorator


def create_test_data(
    conn: duckdb.DuckDBPyConnection, table_name: str, num_rows: int = 1000
) -> None:
    """Create test data for demonstrations."""
    conn.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT 
            range AS id,
            'user_' || range AS username,
            random() * 100 AS score,
            NOW() - INTERVAL (random() * 365) DAY AS created_at
        FROM range({num_rows})
    """
    )
    print(f"✅ Created {table_name} with {num_rows} rows")


def cleanup_ducklake(catalog_path: str) -> None:
    """Clean up DuckLake catalog and data files."""
    # Extract the base path from the catalog path
    if catalog_path.startswith("ducklake:"):
        base_path = catalog_path[9:]  # Remove "ducklake:" prefix
    else:
        base_path = catalog_path

    # Remove catalog file
    if os.path.exists(base_path):
        os.remove(base_path)
        print(f"🗑️  Removed catalog: {base_path}")

    # Remove data files directory
    data_dir = base_path + ".files"
    if os.path.exists(data_dir):
        import shutil

        shutil.rmtree(data_dir)
        print(f"🗑️  Removed data directory: {data_dir}")


def compare_file_counts(path1: str, label1: str, path2: str, label2: str) -> None:
    """Compare file counts between two directories."""

    def count_files(path):
        if not os.path.exists(path):
            return 0
        return len(
            [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        )

    count1 = count_files(path1)
    count2 = count_files(path2)

    print(f"\n📁 File Count Comparison:")
    print(f"   {label1}: {count1} files")
    print(f"   {label2}: {count2} files")
    print(f"   Difference: {abs(count1 - count2)} files")


def format_size(bytes: int) -> str:
    """Format bytes as human-readable size."""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes < 1024.0:
            return f"{bytes:.2f} {unit}"
        bytes /= 1024.0
    return f"{bytes:.2f} TB"


def get_directory_size(path: str) -> int:
    """Get total size of all files in a directory and its subdirectories."""
    total = 0
    if os.path.exists(path):
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                total += os.path.getsize(filepath)
    return total


def compare_storage_size(path1: str, label1: str, path2: str, label2: str) -> None:
    """Compare storage size between two directories."""
    size1 = get_directory_size(path1)
    size2 = get_directory_size(path2)

    print(f"\n💾 Storage Size Comparison:")
    print(f"   {label1}: {format_size(size1)}")
    print(f"   {label2}: {format_size(size2)}")

    if size1 > 0 and size2 > 0:
        ratio = size2 / size1
        print(f"   Ratio: {ratio:.2f}x")
