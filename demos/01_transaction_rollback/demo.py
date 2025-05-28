#!/usr/bin/env python3
"""
Demo 1: Transaction Rollback Safety

This demo showcases DuckLake's ability to maintain transactional consistency
across multiple tables, something traditional lakehouse formats cannot do.

Scenario: E-commerce system where inventory and orders must stay consistent.
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), "../../utils"))

from ducklake_utils import (
    DuckLakeConnection,
    print_section,
    print_query_result,
    show_snapshots,
    cleanup_ducklake,
)


def setup_tables(conn):
    """Create the inventory and orders tables."""
    print("🔧 Setting up tables...")

    # Switch to the lake database
    conn.execute("USE lake")

    # Create inventory table
    conn.execute(
        """
        CREATE TABLE inventory (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR,
            quantity INTEGER,
            price DECIMAL(10, 2)
        )
    """
    )

    # Create orders table
    conn.execute(
        """
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            quantity INTEGER,
            customer_name VARCHAR,
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    # Insert initial inventory
    conn.execute(
        """
        INSERT INTO inventory VALUES 
            (1, 'DuckDB T-Shirt', 100, 29.99),
            (2, 'DuckDB Mug', 50, 14.99),
            (3, 'DuckDB Sticker Pack', 200, 4.99),
            (4, 'DuckDB Hoodie', 25, 59.99)
    """
    )

    print("✅ Tables created and initial data loaded")


def show_current_state(conn):
    """Display the current state of inventory and orders."""
    print_query_result(
        conn, "SELECT * FROM inventory ORDER BY product_id", "Current Inventory"
    )
    print_query_result(conn, "SELECT * FROM orders ORDER BY order_id", "Current Orders")


def demo_successful_transaction(conn):
    """Demonstrate a successful transaction."""
    print_section("Successful Transaction")

    print("📦 Customer 'Alice' orders 5 DuckDB T-Shirts...")

    conn.execute("BEGIN TRANSACTION")

    # Insert order
    conn.execute(
        """
        INSERT INTO orders (order_id, product_id, quantity, customer_name)
        VALUES (1, 1, 5, 'Alice')
    """
    )

    # Update inventory
    conn.execute(
        """
        UPDATE inventory 
        SET quantity = quantity - 5 
        WHERE product_id = 1
    """
    )

    conn.execute("COMMIT")

    print("✅ Transaction committed successfully")
    show_current_state(conn)


def demo_failed_transaction(conn):
    """Demonstrate a failed transaction with rollback."""
    print_section("Failed Transaction with Rollback")

    print("📦 Customer 'Bob' tries to order 10 DuckDB Mugs...")
    print("❌ But accidentally tries to insert duplicate order_id!")

    try:
        conn.execute("BEGIN TRANSACTION")

        # First order - this will succeed
        conn.execute(
            """
            INSERT INTO orders (order_id, product_id, quantity, customer_name)
            VALUES (2, 2, 10, 'Bob')
        """
        )

        # Update inventory
        conn.execute(
            """
            UPDATE inventory 
            SET quantity = quantity - 10 
            WHERE product_id = 2
        """
        )

        # Oops! Duplicate order_id - this will fail
        print("💥 Simulating error: duplicate order_id...")
        conn.execute(
            """
            INSERT INTO orders (order_id, product_id, quantity, customer_name)
            VALUES (2, 3, 5, 'Bob')
        """
        )

        conn.execute("COMMIT")

    except Exception as e:
        print(f"❌ Error occurred: {e}")
        conn.execute("ROLLBACK")
        print("🔄 Transaction rolled back")

    print("\n✅ After rollback, data remains consistent:")
    show_current_state(conn)


def demo_complex_transaction(conn):
    """Demonstrate a complex multi-table transaction."""
    print_section("Complex Multi-Table Transaction")

    print("📦 Processing a bulk order with inventory checks...")

    # Check if we have enough inventory
    result = conn.execute(
        """
        SELECT product_id, product_name, quantity 
        FROM inventory 
        WHERE product_id IN (2, 3, 4)
    """
    ).fetchall()

    print("Current stock levels:")
    for row in result:
        print(f"  - {row[1]}: {row[2]} units")

    try:
        conn.execute("BEGIN TRANSACTION")

        # Multiple orders in one transaction
        orders = [
            (3, 2, 5, "Charlie"),  # 5 Mugs
            (4, 3, 20, "Charlie"),  # 20 Sticker Packs
            (5, 4, 2, "Charlie"),  # 2 Hoodies
        ]

        for order_id, product_id, quantity, customer in orders:
            # Check inventory before inserting order
            available = conn.execute(
                f"""
                SELECT quantity FROM inventory WHERE product_id = {product_id}
            """
            ).fetchone()[0]

            if available < quantity:
                raise ValueError(f"Insufficient inventory for product {product_id}")

            # Insert order
            conn.execute(
                f"""
                INSERT INTO orders (order_id, product_id, quantity, customer_name)
                VALUES ({order_id}, {product_id}, {quantity}, '{customer}')
            """
            )

            # Update inventory
            conn.execute(
                f"""
                UPDATE inventory 
                SET quantity = quantity - {quantity}
                WHERE product_id = {product_id}
            """
            )

        conn.execute("COMMIT")
        print("✅ Complex transaction committed successfully")

    except Exception as e:
        print(f"❌ Error in complex transaction: {e}")
        conn.execute("ROLLBACK")
        print("🔄 All changes rolled back")

    show_current_state(conn)


def demo_time_travel_after_transactions(conn):
    """Show how we can view the database state at different points in time."""
    print_section("Time Travel Through Transaction History")

    # Show all snapshots
    show_snapshots(conn)

    # Query inventory at different versions
    print("\n🕐 Inventory at different points in time:")

    versions = conn.execute(
        """
        SELECT DISTINCT snapshot_id 
        FROM ducklake_snapshots('lake') 
        ORDER BY snapshot_id
        LIMIT 4
    """
    ).fetchall()

    for version in versions:
        version_id = version[0]
        print(f"\n📌 Version {version_id}:")
        result = conn.execute(
            f"""
            SELECT product_name, quantity 
            FROM inventory AT (VERSION => {version_id})
            ORDER BY product_id
        """
        ).fetchall()

        for row in result:
            print(f"  - {row[0]}: {row[1]} units")


def main():
    """Run the transaction rollback safety demo."""
    print_section("Demo 1: Transaction Rollback Safety", 80)

    catalog_path = "ducklake:transaction_demo.ducklake"

    # Clean up any existing catalog
    cleanup_ducklake(catalog_path)

    with DuckLakeConnection(catalog_path) as conn:
        # Setup
        setup_tables(conn)
        show_current_state(conn)

        # Run demos
        demo_successful_transaction(conn)
        demo_failed_transaction(conn)
        demo_complex_transaction(conn)
        demo_time_travel_after_transactions(conn)

    print("\n✅ Demo completed!")
    print("📁 Catalog location: transaction_demo.ducklake")
    print("📁 Data files: transaction_demo.ducklake.files/")


if __name__ == "__main__":
    main()
