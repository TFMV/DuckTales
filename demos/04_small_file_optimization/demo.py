#!/usr/bin/env python3
"""
Demo 4: Small File Optimization

This demo showcases DuckLake's efficiency in handling frequent small updates
compared to traditional lakehouse formats that create many small files.

Scenario: Compare metadata overhead between DuckLake and traditional formats
for IoT sensor data with frequent small updates.
"""

import sys
import os
import time
import shutil
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), "../../utils"))

from ducklake_utils import (
    DuckLakeConnection,
    print_section,
    print_query_result,
    cleanup_ducklake,
    compare_file_counts,
    compare_storage_size,
    format_size,
    get_directory_size,
)


def simulate_traditional_format(base_path, num_updates=100):
    """Simulate how traditional formats handle small updates."""
    print_section("Simulating Traditional Lakehouse Format")

    trad_path = os.path.join(base_path, "traditional_format")
    os.makedirs(trad_path, exist_ok=True)

    # Metadata directory structure
    metadata_path = os.path.join(trad_path, "metadata")
    manifest_path = os.path.join(metadata_path, "manifests")
    snapshot_path = os.path.join(metadata_path, "snapshots")
    data_path = os.path.join(trad_path, "data")

    for path in [metadata_path, manifest_path, snapshot_path, data_path]:
        os.makedirs(path, exist_ok=True)

    print(f"📁 Traditional format directory: {trad_path}")

    # Simulate initial data file
    with open(os.path.join(data_path, "data-00000.parquet"), "w") as f:
        f.write("PARQUET" * 200)  # Simulate 1.4KB parquet file

    # Simulate metadata files for initial state
    with open(os.path.join(snapshot_path, "snapshot-v0.json"), "w") as f:
        f.write(
            '{"version": 0, "timestamp": "2024-01-01", "manifest_list": "manifest-list-0.json"}\n'
            * 10
        )

    with open(os.path.join(metadata_path, "manifest-list-0.json"), "w") as f:
        f.write('{"manifests": ["manifest-0.json"]}\n' * 10)

    with open(os.path.join(manifest_path, "manifest-0.json"), "w") as f:
        f.write('{"data_files": ["data-00000.parquet"], "row_count": 100}\n' * 10)

    print(f"🔄 Simulating {num_updates} small updates...")

    # Simulate small updates
    for i in range(1, num_updates + 1):
        # Each update creates:
        # 1. New data file (even for single row)
        with open(os.path.join(data_path, f"data-{i:05d}.parquet"), "w") as f:
            f.write("PARQUET" * 20)  # Small 140-byte file

        # 2. New manifest file
        with open(os.path.join(manifest_path, f"manifest-{i}.json"), "w") as f:
            manifest_content = (
                f'{{"data_files": ["data-{i:05d}.parquet"], "row_count": 1}}\n'
            )
            f.write(manifest_content * 10)

        # 3. New manifest list
        with open(os.path.join(metadata_path, f"manifest-list-{i}.json"), "w") as f:
            manifests = [f'"manifest-{j}.json"' for j in range(i + 1)]
            manifest_list = f'{{"manifests": [{",".join(manifests)}]}}\n'
            f.write(manifest_list * 10)

        # 4. New snapshot file
        with open(os.path.join(snapshot_path, f"snapshot-v{i}.json"), "w") as f:
            snapshot_content = f'{{"version": {i}, "timestamp": "{datetime.now()}", "manifest_list": "manifest-list-{i}.json"}}\n'
            f.write(snapshot_content * 10)

        if i % 20 == 0:
            print(f"   Processed {i} updates...")

    # Count files
    total_files = 0
    file_breakdown = {}

    for root, dirs, files in os.walk(trad_path):
        total_files += len(files)
        category = os.path.basename(root)
        file_breakdown[category] = len(files)

    print(f"\n📊 Traditional format statistics after {num_updates} updates:")
    print(f"   Total files created: {total_files}")
    print("   File breakdown:")
    for category, count in file_breakdown.items():
        if count > 0:
            print(f"     - {category}: {count} files")

    total_size = get_directory_size(trad_path)
    print(f"   Total storage used: {format_size(total_size)}")

    return trad_path


def simulate_ducklake_format(base_path, num_updates=100):
    """Simulate how DuckLake handles small updates."""
    print_section("DuckLake Format Handling")

    catalog_path = f"ducklake:{os.path.join(base_path, 'ducklake_format.ducklake')}"

    # Clean up any existing catalog
    cleanup_ducklake(catalog_path)

    with DuckLakeConnection(catalog_path) as conn:
        conn.execute("USE lake")

        # Create sensor data table
        conn.execute(
            """
            CREATE TABLE sensor_data (
                sensor_id VARCHAR,
                timestamp TIMESTAMP,
                temperature DOUBLE,
                humidity DOUBLE,
                location VARCHAR
            )
        """
        )

        # Initial data
        conn.execute(
            """
            INSERT INTO sensor_data VALUES 
                ('sensor_001', '2024-01-01 00:00:00', 22.5, 45.0, 'Building A'),
                ('sensor_002', '2024-01-01 00:00:00', 23.1, 42.0, 'Building B')
        """
        )

        print(f"🔄 Simulating {num_updates} small updates...")

        # Simulate small updates
        start_time = time.time()

        for i in range(1, num_updates + 1):
            # Single row insert (typical IoT pattern)
            timestamp = datetime.now() - timedelta(minutes=num_updates - i)
            temp = 20 + (i % 10) * 0.5
            humidity = 40 + (i % 20) * 0.5

            conn.execute(
                f"""
                INSERT INTO sensor_data VALUES 
                ('sensor_{(i % 3) + 1:03d}', '{timestamp}', {temp}, {humidity}, 'Building {chr(65 + (i % 3))}')
            """
            )

            if i % 20 == 0:
                print(f"   Processed {i} updates...")

        elapsed = time.time() - start_time

        # Get statistics
        row_count = conn.execute("SELECT COUNT(*) FROM sensor_data").fetchone()[0]

        # Count actual files created
        data_files_path = catalog_path.replace("ducklake:", "") + ".files"

        if os.path.exists(data_files_path):
            data_files = [
                f for f in os.listdir(data_files_path) if f.endswith(".parquet")
            ]
            num_data_files = len(data_files)
        else:
            num_data_files = 0

        print(f"\n📊 DuckLake statistics after {num_updates} updates:")
        print(f"   Total rows in table: {row_count}")
        print(f"   Data files created: {num_data_files}")
        print(f"   Time taken: {elapsed:.2f} seconds")
        print(f"   Updates per second: {num_updates/elapsed:.1f}")

        # Show snapshot efficiency
        snapshots = conn.execute(
            "SELECT COUNT(*) FROM ducklake_snapshots('lake')"
        ).fetchone()[0]
        print(f"   Snapshots created: {snapshots}")

        # Calculate storage
        catalog_size = (
            os.path.getsize(catalog_path.replace("ducklake:", ""))
            if os.path.exists(catalog_path.replace("ducklake:", ""))
            else 0
        )
        data_size = (
            get_directory_size(data_files_path)
            if os.path.exists(data_files_path)
            else 0
        )
        total_size = catalog_size + data_size

        print(f"   Catalog size: {format_size(catalog_size)}")
        print(f"   Data files size: {format_size(data_size)}")
        print(f"   Total storage used: {format_size(total_size)}")

    return catalog_path.replace("ducklake:", "")


def demonstrate_inlining(base_path):
    """Demonstrate DuckLake's inlining feature."""
    print_section("DuckLake Inlining Feature")

    catalog_path = f"ducklake:{os.path.join(base_path, 'inlining_demo.ducklake')}"

    # Clean up any existing catalog
    cleanup_ducklake(catalog_path)

    with DuckLakeConnection(catalog_path) as conn:
        conn.execute("USE lake")

        # Create a table with small rows
        conn.execute(
            """
            CREATE TABLE small_rows (
                id INTEGER,
                name VARCHAR,
                value INTEGER,
                created_at TIMESTAMP
            )
        """
        )

        # Insert small rows
        print("🔄 Inserting small rows...")
        for i in range(100):
            conn.execute(
                f"""
                INSERT INTO small_rows (id, name, value, created_at)
                VALUES ({i}, 'item_{i}', {i * 10}, CURRENT_TIMESTAMP)
            """
            )

        # Show statistics
        row_count = conn.execute("SELECT COUNT(*) FROM small_rows").fetchone()[0]
        print(f"\n📊 Table Statistics:")
        print(f"   Total rows: {row_count}")

        # Show storage efficiency
        catalog_size = os.path.getsize(catalog_path.replace("ducklake:", ""))
        data_files_size = get_directory_size(
            catalog_path.replace("ducklake:", "") + ".files"
        )
        total_size = catalog_size + data_files_size

        print("\n💾 Storage Usage:")
        print(f"   Catalog size: {format_size(catalog_size)}")
        print(f"   Data files size: {format_size(data_files_size)}")
        print(f"   Total size: {format_size(total_size)}")

        # Show average row size
        if row_count > 0:
            avg_row_size = total_size / row_count
            print(f"   Average bytes per row: {avg_row_size:.1f}")


def performance_comparison(base_path):
    """Compare performance between traditional and DuckLake formats."""
    print_section("Performance Comparison")

    # Test with different update counts
    test_sizes = [10, 50, 100]

    results = []

    for size in test_sizes:
        print(f"\n🔬 Testing with {size} updates...")

        # Traditional format
        trad_start = time.time()
        trad_path = simulate_traditional_format(
            os.path.join(base_path, f"perf_test_{size}"), size
        )
        trad_time = time.time() - trad_start
        trad_files = sum(1 for _ in os.walk(trad_path) for _ in _[2])
        trad_size = get_directory_size(trad_path)

        # DuckLake format
        duck_start = time.time()
        duck_path = simulate_ducklake_format(
            os.path.join(base_path, f"perf_test_{size}"), size
        )
        duck_time = time.time() - duck_start

        duck_files = 1  # Catalog file
        data_files_path = duck_path + ".files"
        if os.path.exists(data_files_path):
            duck_files += len(os.listdir(data_files_path))

        duck_size = os.path.getsize(duck_path) if os.path.exists(duck_path) else 0
        if os.path.exists(data_files_path):
            duck_size += get_directory_size(data_files_path)

        results.append(
            {
                "updates": size,
                "trad_time": trad_time,
                "trad_files": trad_files,
                "trad_size": trad_size,
                "duck_time": duck_time,
                "duck_files": duck_files,
                "duck_size": duck_size,
            }
        )

        # Clean up test directories
        shutil.rmtree(os.path.join(base_path, f"perf_test_{size}"), ignore_errors=True)

    # Display results
    print("\n📊 Performance Comparison Results:")
    print("=" * 80)
    print(f"{'Updates':<10} {'Traditional':<30} {'DuckLake':<30} {'Improvement':<20}")
    print(
        f"{'':10} {'Time | Files | Size':<30} {'Time | Files | Size':<30} {'Files | Size':<20}"
    )
    print("-" * 80)

    for r in results:
        trad_str = (
            f"{r['trad_time']:.1f}s | {r['trad_files']} | {format_size(r['trad_size'])}"
        )
        duck_str = (
            f"{r['duck_time']:.1f}s | {r['duck_files']} | {format_size(r['duck_size'])}"
        )

        file_reduction = ((r["trad_files"] - r["duck_files"]) / r["trad_files"]) * 100
        size_reduction = ((r["trad_size"] - r["duck_size"]) / r["trad_size"]) * 100

        improvement = f"{file_reduction:.0f}% | {size_reduction:.0f}%"

        print(f"{r['updates']:<10} {trad_str:<30} {duck_str:<30} {improvement:<20}")


def main():
    """Run the small file optimization demo."""
    print_section("Demo 4: Small File Optimization", 80)

    # Create base directory for demo
    base_path = "small_file_demo"
    os.makedirs(base_path, exist_ok=True)

    try:
        # Run simulations
        trad_path = simulate_traditional_format(base_path, 100)
        duck_path = simulate_ducklake_format(base_path, 100)

        # Compare results
        print_section("Comparison Summary")

        # File count comparison
        trad_files = sum(1 for _ in os.walk(trad_path) for _ in _[2])

        duck_files = 1  # Catalog
        data_files_path = duck_path + ".files"
        if os.path.exists(data_files_path):
            duck_files += len(os.listdir(data_files_path))

        print(f"📁 File Count:")
        print(f"   Traditional Format: {trad_files} files")
        print(f"   DuckLake Format: {duck_files} files")
        print(f"   Reduction: {((trad_files - duck_files) / trad_files * 100):.1f}%")

        # Storage comparison
        trad_size = get_directory_size(trad_path)
        duck_size = os.path.getsize(duck_path) if os.path.exists(duck_path) else 0
        if os.path.exists(data_files_path):
            duck_size += get_directory_size(data_files_path)

        print(f"\n💾 Storage Size:")
        print(f"   Traditional Format: {format_size(trad_size)}")
        print(f"   DuckLake Format: {format_size(duck_size)}")
        print(f"   Reduction: {((trad_size - duck_size) / trad_size * 100):.1f}%")

        # Demonstrate inlining
        demonstrate_inlining(base_path)

        # Performance comparison
        performance_comparison(base_path)

    finally:
        # Clean up
        print("\n🧹 Cleaning up demo files...")
        shutil.rmtree(base_path, ignore_errors=True)

    print("\n✅ Demo completed!")
    print(
        "\n💡 Key Takeaway: DuckLake dramatically reduces file count and storage overhead for frequent small updates!"
    )


if __name__ == "__main__":
    main()
