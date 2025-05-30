#!/usr/bin/env python3
"""
Demo 2: Time Travel Debugging

This demo showcases DuckLake's time travel capabilities for debugging data issues,
investigating when changes occurred, and recovering accidentally deleted data.

Scenario: Customer data was accidentally deleted. When did it happen? Can we recover it?
"""

import sys
import os
import time
import random
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), "../../utils"))

from ducklake_utils import (
    DuckLakeConnection,
    print_section,
    print_query_result,
    show_snapshots,
    show_table_changes,
    cleanup_ducklake,
)


def setup_customer_data(conn):
    """Create and populate the customers table."""
    print("🔧 Setting up customer data...")

    conn.execute("USE lake")

    # Create customers table
    conn.execute(
        """
        CREATE TABLE customers (
            id INTEGER,
            name VARCHAR,
            email VARCHAR,
            phone VARCHAR,
            created_at TIMESTAMP,
            last_order_date DATE,
            total_spent DECIMAL(10, 2)
        )
    """
    )

    # Insert initial customer data
    conn.execute(
        """
        INSERT INTO customers (id, name, email, phone, last_order_date, total_spent) VALUES 
            (1, 'Alice Johnson', 'alice@example.com', '555-0101', '2024-01-15', 1250.50),
            (2, 'Bob Smith', 'bob@example.com', '555-0102', '2024-01-10', 890.25),
            (3, 'Charlie Brown', 'charlie@example.com', '555-0103', '2024-01-20', 2100.00),
            (4, 'Diana Prince', 'diana@example.com', '555-0104', '2024-01-18', 750.75),
            (5, 'Eve Wilson', 'eve@example.com', '555-0105', '2024-01-22', 1500.00)
    """
    )

    print("✅ Customer table created with 5 customers")

    # Take a snapshot after initial load
    conn.execute("BEGIN TRANSACTION")
    conn.execute("COMMIT")


def simulate_normal_operations(conn):
    """Simulate normal business operations with updates."""
    print_section("Simulating Normal Operations")

    operations = [
        (
            "UPDATE customers SET total_spent = total_spent + 150.00 WHERE id = 1",
            "Alice made a new purchase",
        ),
        (
            "INSERT INTO customers (id, name, email, phone, last_order_date, total_spent) VALUES (6, 'Frank Miller', 'frank@example.com', '555-0106', '2024-01-23', 500.00)",
            "New customer Frank joined",
        ),
        (
            "UPDATE customers SET email = 'alice.johnson@example.com' WHERE id = 1",
            "Alice updated her email",
        ),
        (
            "UPDATE customers SET last_order_date = '2024-01-25' WHERE id = 2",
            "Bob made a new order",
        ),
    ]

    for sql, description in operations:
        print(f"📝 {description}")
        conn.execute("BEGIN TRANSACTION")
        conn.execute(sql)
        conn.execute("COMMIT")  # Create a snapshot for each operation
        time.sleep(0.5)  # Small delay to show time progression

    print("✅ Normal operations completed")


def simulate_accidental_deletion(conn):
    """Simulate an accidental deletion of customer data."""
    print_section("Accidental Data Deletion")

    print("❌ OH NO! Someone accidentally ran a DELETE without WHERE clause!")
    print("   Executing: DELETE FROM customers WHERE name LIKE 'B%'")
    print("   But they forgot the WHERE clause...")

    # Simulate the accident
    conn.execute("BEGIN TRANSACTION")
    conn.execute("DELETE FROM customers")  # Oops! No WHERE clause
    conn.execute("COMMIT")

    print("\n💥 All customer data has been deleted!")
    print_query_result(
        conn,
        "SELECT COUNT(*) as remaining_customers FROM customers",
        "Remaining Customers",
    )


def investigate_what_happened(conn):
    """Use time travel to investigate what happened."""
    print_section("Investigation: What Happened?")

    # Show all snapshots
    show_snapshots(conn)

    # Get snapshot information
    snapshots = conn.execute(
        """
        SELECT snapshot_id, snapshot_time, changes
        FROM ducklake_snapshots('lake')
        ORDER BY snapshot_id DESC
        LIMIT 10
    """
    ).fetchall()

    print("\n🔍 Analyzing recent changes...")

    # Find the deletion snapshot
    deletion_snapshot = None
    previous_snapshot = None

    for i, (snap_id, snap_time, changes) in enumerate(snapshots):
        changes_str = str(changes)
        if "tables_deleted_from" in changes_str:
            deletion_snapshot = snap_id
            if i + 1 < len(snapshots):
                previous_snapshot = snapshots[i + 1][0]
            break

    if deletion_snapshot:
        print(f"\n⚠️  Found deletion in snapshot {deletion_snapshot}")
        print(f"   Previous good snapshot: {previous_snapshot}")

        # Show what was deleted
        if previous_snapshot:
            print("\n📊 Data that was deleted:")
            deleted_data = conn.execute(
                f"""
                SELECT * FROM customers AT (VERSION => {previous_snapshot})
                ORDER BY id
            """
            ).fetchdf()
            print(deleted_data.to_string())

    return previous_snapshot


def demonstrate_time_travel_queries(conn, good_snapshot):
    """Show various time travel query capabilities."""
    print_section("Time Travel Query Examples")

    if not good_snapshot:
        print("❌ No good snapshot found")
        return

    # Query at specific version
    print(f"\n1️⃣ Query at specific version (snapshot {good_snapshot}):")
    print_query_result(
        conn,
        f"SELECT id, name, email FROM customers AT (VERSION => {good_snapshot}) ORDER BY id",
        "Customers at Good Snapshot",
    )

    # Query at specific timestamp
    print("\n2️⃣ Query at specific timestamp (5 minutes ago):")
    five_min_ago = datetime.now() - timedelta(minutes=5)
    timestamp_str = five_min_ago.strftime("%Y-%m-%d %H:%M:%S")

    try:
        print_query_result(
            conn,
            f"SELECT COUNT(*) as customer_count FROM customers AT (TIMESTAMP => '{timestamp_str}')",
            "Customer Count 5 Minutes Ago",
        )
    except:
        print("   (Timestamp might be before table creation)")

    # Show changes between versions
    print("\n3️⃣ Track changes between versions:")
    current_snapshot = conn.execute(
        "SELECT MAX(snapshot_id) FROM ducklake_snapshots('lake')"
    ).fetchone()[0]

    if good_snapshot and current_snapshot > good_snapshot:
        show_table_changes(
            conn, "lake", "main", "customers", good_snapshot, current_snapshot
        )


def recover_deleted_data(conn, good_snapshot):
    """Recover the accidentally deleted data."""
    print_section("Data Recovery")

    if not good_snapshot:
        print("❌ Cannot recover - no good snapshot found")
        return

    print(f"🔄 Recovering data from snapshot {good_snapshot}...")

    # Count records before recovery
    current_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    print(f"   Records before recovery: {current_count}")

    conn.execute("BEGIN TRANSACTION")
    conn.execute(
        f"""
        INSERT INTO customers 
        SELECT * FROM customers AT (VERSION => {good_snapshot})
    """
    )
    conn.execute("COMMIT")

    # Count records after recovery
    after_count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    print(f"   Records after recovery: {after_count}")
    print(f"   Recovered records: {after_count - current_count}")

    print("\n✅ Data successfully recovered!")
    print_query_result(
        conn, "SELECT * FROM customers ORDER BY id", "Recovered Customer Data"
    )


def demonstrate_advanced_time_travel(conn):
    """Show advanced time travel features."""
    print_section("Advanced Time Travel Features")

    print("📋 Creating audit log of all changes to customer emails:")

    # Get all snapshots
    snapshots = conn.execute(
        """
        SELECT snapshot_id 
        FROM ducklake_snapshots('lake')
        WHERE snapshot_id > 1
        ORDER BY snapshot_id
        """
    ).fetchall()

    # Track email changes across snapshots
    email_changes = []
    for i in range(1, len(snapshots)):
        prev_snap = snapshots[i - 1][0]
        curr_snap = snapshots[i][0]

        # Compare emails between snapshots
        changes = conn.execute(
            f"""
            WITH prev_state AS (
                SELECT id, name, email 
                FROM main.customers AT (VERSION => {prev_snap})
            ),
            curr_state AS (
                SELECT id, name, email 
                FROM main.customers AT (VERSION => {curr_snap})
            )
            SELECT 
                curr_state.id,
                curr_state.name,
                prev_state.email as old_email,
                curr_state.email as new_email,
                {curr_snap} as snapshot_id
            FROM prev_state
            JOIN curr_state ON prev_state.id = curr_state.id
            WHERE prev_state.email != curr_state.email
            """
        ).fetchall()

        email_changes.extend(changes)

    if email_changes:
        print("\n📧 Email change history:")
        for change in email_changes:
            print(
                f"   Customer {change[0]} ({change[1]}): {change[2]} → {change[3]} (snapshot {change[4]})"
            )
    else:
        print("   No email changes found in history")

    print("\n✅ Advanced time travel demo completed!")


def main():
    """Run the time travel debugging demo."""
    print_section("Demo 2: Time Travel Debugging", 80)

    catalog_path = "ducklake:timetravel_demo.ducklake"

    # Clean up any existing catalog
    cleanup_ducklake(catalog_path)

    with DuckLakeConnection(catalog_path) as conn:
        # Setup and simulate operations
        setup_customer_data(conn)
        simulate_normal_operations(conn)

        # Show current state
        print_query_result(
            conn,
            "SELECT * FROM customers ORDER BY id",
            "Current Customer Data (Before Accident)",
        )

        # Simulate the accident
        simulate_accidental_deletion(conn)

        # Investigate and recover
        good_snapshot = investigate_what_happened(conn)
        demonstrate_time_travel_queries(conn, good_snapshot)
        recover_deleted_data(conn, good_snapshot)
        demonstrate_advanced_time_travel(conn)

    print("\n✅ Demo completed!")
    print("📁 Catalog location: timetravel_demo.ducklake")
    print("📁 Data files: timetravel_demo.ducklake.files/")
    print(
        "\n💡 Key Takeaway: DuckLake's time travel makes data recovery simple and investigation straightforward!"
    )


if __name__ == "__main__":
    main()
