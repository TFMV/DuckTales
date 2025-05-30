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

    # Simulate initial data file with more realistic parquet overhead
    with open(os.path.join(data_path, "data-00000.parquet"), "w") as f:
        # Simulate a more realistic parquet file with headers, metadata, and row groups
        f.write("PAR1" * 100)  # File header
        f.write("META" * 100)  # File metadata
        f.write("DATA" * 200)  # Row group 1
        f.write("DICT" * 50)  # Dictionary encoding
        f.write("PAR1" * 100)  # File footer

    # Simulate metadata files for initial state with realistic JSON content
    with open(os.path.join(snapshot_path, "snapshot-v0.json"), "w") as f:
        snapshot_content = {
            "version": 0,
            "timestamp": "2024-01-01T00:00:00Z",
            "manifest_list": "manifest-list-0.json",
            "summary": {
                "total_records": 1000,
                "total_files": 1,
                "column_stats": {"col1": {"min": 0, "max": 100, "null_count": 0}},
            },
        }
        f.write(str(snapshot_content) * 5)  # Realistic JSON size

    with open(os.path.join(metadata_path, "manifest-list-0.json"), "w") as f:
        manifest_list = {
            "manifests": ["manifest-0.json"],
            "schema_id": 0,
            "created_at": "2024-01-01T00:00:00Z",
        }
        f.write(str(manifest_list) * 5)

    with open(os.path.join(manifest_path, "manifest-0.json"), "w") as f:
        manifest = {
            "data_files": ["data-00000.parquet"],
            "row_count": 1000,
            "column_stats": {"col1": {"min": 0, "max": 100, "null_count": 0}},
            "created_at": "2024-01-01T00:00:00Z",
        }
        f.write(str(manifest) * 5)

    print(f"🔄 Simulating {num_updates} small updates...")

    # Simulate small updates with realistic file sizes
    for i in range(1, num_updates + 1):
        # Each update creates new files with realistic sizes

        # 1. New data file with parquet overhead
        with open(os.path.join(data_path, f"data-{i:05d}.parquet"), "w") as f:
            f.write("PAR1" * 50)  # File header
            f.write("META" * 50)  # File metadata
            f.write("DATA" * 100)  # Row group 1
            f.write("DICT" * 25)  # Dictionary encoding
            f.write("PAR1" * 50)  # File footer

        # 2. New manifest file with detailed stats
        with open(os.path.join(manifest_path, f"manifest-{i}.json"), "w") as f:
            manifest = {
                "data_files": [f"data-{i:05d}.parquet"],
                "row_count": 100,
                "column_stats": {
                    "col1": {"min": i, "max": i + 100, "null_count": 0},
                    "col2": {"min": f"val_{i}", "max": f"val_{i+100}", "null_count": 0},
                },
                "created_at": datetime.now().isoformat(),
            }
            f.write(str(manifest) * 2)

        # 3. New manifest list with cumulative stats
        with open(os.path.join(metadata_path, f"manifest-list-{i}.json"), "w") as f:
            manifest_list = {
                "manifests": [f"manifest-{j}.json" for j in range(i + 1)],
                "schema_id": 0,
                "created_at": datetime.now().isoformat(),
                "statistics": {"record_count": (i + 1) * 100, "file_count": i + 1},
            }
            f.write(str(manifest_list) * 2)

        # 4. New snapshot file with table stats
        with open(os.path.join(snapshot_path, f"snapshot-v{i}.json"), "w") as f:
            snapshot = {
                "version": i,
                "timestamp": datetime.now().isoformat(),
                "manifest_list": f"manifest-list-{i}.json",
                "summary": {
                    "total_records": (i + 1) * 100,
                    "total_files": i + 1,
                    "column_stats": {
                        "col1": {"min": 0, "max": i + 100, "null_count": 0},
                        "col2": {
                            "min": "val_0",
                            "max": f"val_{i+100}",
                            "null_count": 0,
                        },
                    },
                },
            }
            f.write(str(snapshot) * 2)

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


def get_ducklake_size(catalog_path):
    """Calculate the total size of a DuckLake catalog including data files."""
    base_path = catalog_path.replace("ducklake:", "")
    total_size = 0

    # Add catalog file size
    if os.path.exists(base_path):
        try:
            total_size += os.path.getsize(base_path)
        except (OSError, IOError):
            pass

    # Add data files size
    data_files_path = base_path + ".files"
    total_size += get_directory_size(data_files_path)

    return total_size


def calculate_improvement(traditional_size, ducklake_size):
    """Calculate the percentage improvement (reduction) in size."""
    if traditional_size == 0:
        return 0
    return ((traditional_size - ducklake_size) / traditional_size) * 100


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

        # Calculate storage using the new method
        total_size = get_ducklake_size(catalog_path)
        catalog_size = (
            os.path.getsize(catalog_path.replace("ducklake:", ""))
            if os.path.exists(catalog_path.replace("ducklake:", ""))
            else 0
        )
        data_size = total_size - catalog_size

        print(f"   Catalog size: {format_size(catalog_size)}")
        print(f"   Data files size: {format_size(data_size)}")
        print(f"   Total storage used: {format_size(total_size)}")

        # Add debug information
        print("\n   Debug Information:")
        print(
            f"   - Base path exists: {os.path.exists(catalog_path.replace('ducklake:', ''))}"
        )
        data_files_path = catalog_path.replace("ducklake:", "") + ".files"
        print(f"   - Data files path exists: {os.path.exists(data_files_path)}")
        if os.path.exists(data_files_path):
            parquet_files = [
                f for f in os.listdir(data_files_path) if f.endswith(".parquet")
            ]
            print(f"   - Number of parquet files: {len(parquet_files)}")
            print(
                f"   - Parquet files: {', '.join(parquet_files[:5])}{'...' if len(parquet_files) > 5 else ''}"
            )

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
    print("\n")

    test_sizes = [10, 50, 100]
    results = []

    print("🔬 Testing with different update counts...")

    for num_updates in test_sizes:
        test_dir = os.path.join(base_path, f"perf_test_{num_updates}")
        os.makedirs(test_dir, exist_ok=True)

        # Traditional format
        trad_start = time.time()
        trad_path = simulate_traditional_format(test_dir, num_updates)
        trad_time = time.time() - trad_start
        trad_size = get_directory_size(trad_path)
        trad_files = sum([len(files) for _, _, files in os.walk(trad_path)])

        # DuckLake format
        duck_start = time.time()
        duck_path = simulate_ducklake_format(test_dir, num_updates)
        duck_time = time.time() - duck_start
        duck_size = get_ducklake_size(f"ducklake:{duck_path}")
        duck_files = sum([len(files) for _, _, files in os.walk(duck_path + ".files")])

        # Calculate improvements
        file_improvement = calculate_improvement(trad_files, duck_files)
        size_improvement = calculate_improvement(trad_size, duck_size)

        results.append(
            {
                "updates": num_updates,
                "trad_time": trad_time,
                "trad_files": trad_files,
                "trad_size": trad_size,
                "duck_time": duck_time,
                "duck_files": duck_files,
                "duck_size": duck_size,
                "file_improvement": file_improvement,
                "size_improvement": size_improvement,
            }
        )

    # Print results table
    print("\n📊 Performance Comparison Results:")
    print("=" * 80)
    print(
        "Updates    Traditional                    DuckLake                       Improvement         "
    )
    print(
        "           Time | Files | Size            Time | Files | Size            Files | Size        "
    )
    print("-" * 80)

    for r in results:
        print(
            f"{r['updates']:<10} "
            f"{r['trad_time']:.1f}s | {r['trad_files']} | {format_size(r['trad_size']):<14} "
            f"{r['duck_time']:.1f}s | {r['duck_files']} | {format_size(r['duck_size']):<14} "
            f"{r['file_improvement']:.0f}% | {r['size_improvement']:.0f}%"
        )

    # Add detailed size breakdown for verification
    print("\n📊 Detailed Size Breakdown:")
    print("=" * 80)
    for r in results:
        print(f"\nTest with {r['updates']} updates:")
        print(f"Traditional Format:")
        print(f"  - Total size: {format_size(r['trad_size'])}")
        print(f"  - Files: {r['trad_files']}")
        print(f"DuckLake Format:")
        print(f"  - Total size: {format_size(r['duck_size'])}")
        print(f"  - Files: {r['duck_files']}")
        print(f"Improvement:")
        print(f"  - File count reduction: {r['file_improvement']:.1f}%")
        print(f"  - Storage reduction: {r['size_improvement']:.1f}%")


def compare_formats(base_path):
    """Compare the traditional and DuckLake formats."""
    print_section("Comparison Summary")

    # Get paths
    trad_path = os.path.join(base_path, "traditional_format")
    duck_path = os.path.join(base_path, "ducklake_format.ducklake")

    # File count comparison
    trad_files = sum([len(files) for _, _, files in os.walk(trad_path)])

    duck_files = 1  # Catalog file
    data_files_path = duck_path + ".files"
    if os.path.exists(data_files_path):
        duck_files += sum([len(files) for _, _, files in os.walk(data_files_path)])

    print(f"📁 File Count:")
    print(f"   Traditional Format: {trad_files} files")
    print(f"   DuckLake Format: {duck_files} files")

    file_improvement = calculate_improvement(trad_files, duck_files)
    print(f"   Reduction: {file_improvement:.1f}%")

    # Storage comparison
    trad_size = get_directory_size(trad_path)
    duck_size = get_ducklake_size(f"ducklake:{duck_path}")

    print(f"\n💾 Storage Size:")
    print(f"   Traditional Format: {format_size(trad_size)}")
    print(f"   DuckLake Format: {format_size(duck_size)}")

    size_improvement = calculate_improvement(trad_size, duck_size)
    print(f"   Reduction: {size_improvement:.1f}%")


def main():
    """Run the small file optimization demo."""
    print_section("Demo 4: Small File Optimization")
    print("\n")

    # Create demo directory
    base_path = "small_file_demo"
    os.makedirs(base_path, exist_ok=True)

    # Add explanation of storage size considerations
    print("Note on Storage Size Comparison:")
    print("--------------------------------")
    print(
        "The storage size comparison in this demo shows the raw disk usage of both formats."
    )
    print("DuckLake's storage may appear larger due to:")
    print("1. Additional metadata for time travel and versioning capabilities")
    print("2. Parquet file overhead for small updates")
    print("3. Maintenance of a complete catalog for ACID guarantees")
    print("The trade-off is between storage size and these advanced features.")
    print(
        "In real-world scenarios with larger data volumes, these overheads become negligible."
    )
    print("\n")

    # Run comparisons
    simulate_traditional_format(base_path)
    simulate_ducklake_format(base_path)
    compare_formats(base_path)
    demonstrate_inlining(base_path)
    performance_comparison(base_path)

    # Clean up
    print("\n🧹 Cleaning up demo files...")
    shutil.rmtree(base_path, ignore_errors=True)

    print("\n✅ Demo completed!")
    print(
        "\n💡 Key Takeaway: While DuckLake may use more storage for very small datasets due to feature overhead,"
    )
    print(
        "   it provides significant benefits in terms of data management, versioning, and ACID guarantees."
    )


if __name__ == "__main__":
    main()
