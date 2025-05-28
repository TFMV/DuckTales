#!/usr/bin/env python3
"""
Demo 3: Schema Evolution Without Downtime

This demo showcases DuckLake's ability to perform schema changes transactionally,
allowing for zero-downtime schema evolution that traditional lakehouse formats cannot achieve.

Scenario: Add new columns, change data types, and evolve schemas while applications continue to run.
"""

import sys
import os
import time
import threading
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "../../utils"))

from ducklake_utils import (
    DuckLakeConnection,
    print_section,
    print_query_result,
    show_snapshots,
    cleanup_ducklake,
)


# Global flag to control the background writer
keep_writing = True


def background_writer(catalog_path):
    """Simulate an application continuously writing to the database."""
    global keep_writing
    write_count = 0

    print("🚀 Starting background writer (simulating production traffic)...")

    while keep_writing:
        try:
            with DuckLakeConnection(catalog_path) as conn:
                conn.execute("USE lake")

                # Try to insert an event
                conn.execute(
                    f"""
                    INSERT INTO events (event_type, event_data)
                    VALUES ('background_write', '{{"count": {write_count}, "timestamp": "{datetime.now()}"}}')
                """
                )

                write_count += 1

                if write_count % 10 == 0:
                    print(f"   📝 Background writer: {write_count} events written")

        except Exception as e:
            # Handle schema changes gracefully
            if "Column" in str(e) or "column" in str(e):
                print(f"   🔄 Background writer adapting to schema change...")
                time.sleep(0.5)
            else:
                print(f"   ⚠️  Background writer error: {e}")

        time.sleep(0.1)  # Write 10 events per second

    print(f"✅ Background writer stopped. Total events written: {write_count}")


def setup_initial_schema(conn):
    """Create the initial events table."""
    print("🔧 Setting up initial schema...")

    conn.execute("USE lake")

    # Create events table with minimal schema
    conn.execute(
        """
        CREATE TABLE events (
            id INTEGER PRIMARY KEY DEFAULT nextval('event_id_seq'),
            event_type VARCHAR NOT NULL,
            event_data VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Create sequence for auto-increment
    conn.execute("CREATE SEQUENCE event_id_seq START 1")

    # Insert some initial data
    conn.execute(
        """
        INSERT INTO events (event_type, event_data) VALUES 
            ('user_login', '{"user_id": 123}'),
            ('page_view', '{"page": "/home"}'),
            ('user_logout', '{"user_id": 123}')
    """
    )

    print("✅ Initial schema created")
    print_query_result(conn, "SELECT * FROM events ORDER BY id", "Initial Events Table")


def add_column_with_default(conn):
    """Add a new column with a default value."""
    print_section("Schema Change 1: Add Column with Default")

    print("📋 Current schema:")
    print_query_result(conn, "DESCRIBE events", "Before Adding Column")

    print("\n🔧 Adding 'priority' column with default value...")

    # This is transactional - either succeeds completely or rolls back
    conn.execute("BEGIN TRANSACTION")
    conn.execute("ALTER TABLE events ADD COLUMN priority INTEGER DEFAULT 5")
    conn.execute("COMMIT")

    print("✅ Column added successfully")

    # Show the new schema
    print_query_result(conn, "DESCRIBE events", "After Adding Column")

    # Query showing old and new rows
    print_query_result(
        conn,
        "SELECT id, event_type, priority FROM events ORDER BY id DESC LIMIT 10",
        "Events with New Priority Column",
    )


def add_column_computed(conn):
    """Add a computed column based on existing data."""
    print_section("Schema Change 2: Add Computed Column")

    print("🔧 Adding 'event_category' column based on event_type...")

    conn.execute("BEGIN TRANSACTION")

    # Add the column
    conn.execute("ALTER TABLE events ADD COLUMN event_category VARCHAR")

    # Populate it based on existing data
    conn.execute(
        """
        UPDATE events 
        SET event_category = CASE 
            WHEN event_type LIKE 'user_%' THEN 'user_activity'
            WHEN event_type LIKE 'page_%' THEN 'navigation'
            WHEN event_type = 'background_write' THEN 'system'
            ELSE 'other'
        END
    """
    )

    conn.execute("COMMIT")

    print("✅ Computed column added and populated")

    print_query_result(
        conn,
        """
        SELECT event_category, COUNT(*) as count 
        FROM events 
        GROUP BY event_category 
        ORDER BY count DESC
        """,
        "Event Categories Distribution",
    )


def change_column_type(conn):
    """Change a column's data type."""
    print_section("Schema Change 3: Change Column Type")

    print("🔧 Changing event_data from VARCHAR to JSON...")

    # First, let's ensure all existing data is valid JSON
    conn.execute("BEGIN TRANSACTION")

    # Update any non-JSON data to be valid JSON
    conn.execute(
        """
        UPDATE events 
        SET event_data = '{}' 
        WHERE event_data IS NULL OR event_data = ''
    """
    )

    # Now change the column type
    # Note: DuckDB might require recreating the column
    conn.execute("ALTER TABLE events ADD COLUMN event_data_json JSON")
    conn.execute("UPDATE events SET event_data_json = event_data::JSON")
    conn.execute("ALTER TABLE events DROP COLUMN event_data")
    conn.execute("ALTER TABLE events RENAME COLUMN event_data_json TO event_data")

    conn.execute("COMMIT")

    print("✅ Column type changed to JSON")

    # Demonstrate JSON queries
    print_query_result(
        conn,
        """
        SELECT 
            id,
            event_type,
            json_extract_string(event_data, '$.user_id') as user_id,
            json_extract_string(event_data, '$.count') as count
        FROM events 
        WHERE event_data IS NOT NULL
        LIMIT 10
        """,
        "Querying JSON Data",
    )


def add_constraints(conn):
    """Add constraints to existing columns."""
    print_section("Schema Change 4: Add Constraints")

    print("🔧 Adding NOT NULL constraint to event_category...")

    conn.execute("BEGIN TRANSACTION")

    # First ensure no NULL values
    conn.execute(
        """
        UPDATE events 
        SET event_category = 'uncategorized' 
        WHERE event_category IS NULL
    """
    )

    # Add the constraint
    conn.execute("ALTER TABLE events ALTER COLUMN event_category SET NOT NULL")

    conn.execute("COMMIT")

    print("✅ Constraint added successfully")


def demonstrate_schema_versioning(conn):
    """Show how different schema versions can be queried."""
    print_section("Schema Version Time Travel")

    # Get snapshots
    snapshots = conn.execute(
        """
        SELECT snapshot_id, snapshot_time, schema_version 
        FROM ducklake_snapshots('lake')
        ORDER BY snapshot_id
    """
    ).fetchall()

    print("📋 Schema versions over time:")
    for snap_id, snap_time, schema_ver in snapshots[-5:]:  # Last 5 snapshots
        print(f"   Snapshot {snap_id}: Schema version {schema_ver} at {snap_time}")

    # Query data with old schema
    if len(snapshots) > 2:
        old_snapshot = snapshots[2][0]  # Get an early snapshot

        print(f"\n🕐 Querying with old schema (snapshot {old_snapshot}):")
        try:
            old_columns = conn.execute(
                f"""
                SELECT * FROM events AT (VERSION => {old_snapshot}) LIMIT 1
            """
            ).description

            print("   Columns in old schema:", [col[0] for col in old_columns])
        except:
            print("   (Schema too different to query directly)")

    # Show current schema
    print("\n📋 Current schema columns:")
    current_columns = conn.execute("SELECT * FROM events LIMIT 1").description
    print("   ", [col[0] for col in current_columns])


def create_view_example(conn):
    """Create a view to abstract schema changes."""
    print_section("Schema Change 5: Create Abstraction View")

    print("🔧 Creating a view to provide stable interface...")

    conn.execute(
        """
        CREATE OR REPLACE VIEW events_summary AS
        SELECT 
            id,
            event_type,
            event_category,
            priority,
            created_at,
            CASE 
                WHEN json_valid(event_data) THEN json_extract_string(event_data, '$.user_id')
                ELSE NULL
            END as user_id
        FROM events
    """
    )

    print("✅ View created")

    print_query_result(
        conn,
        "SELECT * FROM events_summary ORDER BY id DESC LIMIT 10",
        "Events Through Stable View Interface",
    )


def main():
    """Run the schema evolution demo."""
    print_section("Demo 3: Schema Evolution Without Downtime", 80)

    catalog_path = "ducklake:schema_evolution_demo.ducklake"

    # Clean up any existing catalog
    cleanup_ducklake(catalog_path)

    # Start background writer thread
    global keep_writing
    keep_writing = True
    writer_thread = threading.Thread(target=background_writer, args=(catalog_path,))
    writer_thread.start()

    try:
        with DuckLakeConnection(catalog_path) as conn:
            # Setup and evolve schema
            setup_initial_schema(conn)

            # Let background writer work a bit
            time.sleep(2)

            # Perform schema changes while writer is active
            add_column_with_default(conn)
            time.sleep(1)

            add_column_computed(conn)
            time.sleep(1)

            change_column_type(conn)
            time.sleep(1)

            add_constraints(conn)
            time.sleep(1)

            create_view_example(conn)

            demonstrate_schema_versioning(conn)

            # Show final state
            print_section("Final State")
            print_query_result(
                conn,
                "SELECT COUNT(*) as total_events FROM events",
                "Total Events (including background writes)",
            )

    finally:
        # Stop background writer
        keep_writing = False
        writer_thread.join()

    print("\n✅ Demo completed!")
    print("📁 Catalog location: schema_evolution_demo.ducklake")
    print("📁 Data files: schema_evolution_demo.ducklake.files/")
    print(
        "\n💡 Key Takeaway: DuckLake enables zero-downtime schema evolution with full ACID guarantees!"
    )


if __name__ == "__main__":
    main()
