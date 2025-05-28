#!/usr/bin/env python3
"""
Demo 5: Catalog Portability

This demo showcases DuckLake's ability to seamlessly transition from local to production
with different catalog backends (local DuckDB, PostgreSQL, MySQL, etc.).

Scenario: Start with local development, then move to production with external database.
"""

import sys
import os
import subprocess
import time
import psycopg2
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), "../../utils"))

from ducklake_utils import (
    DuckLakeConnection,
    print_section,
    print_query_result,
    cleanup_ducklake,
    format_size,
)


def check_postgres_available():
    """Check if PostgreSQL is available for the demo."""
    try:
        # Try to connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", "postgres"),
        )
        conn.close()
        return True
    except:
        return False


def setup_postgres_catalog():
    """Set up PostgreSQL for use as a DuckLake catalog."""
    print("🐘 Setting up PostgreSQL catalog...")

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", "postgres"),
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Create catalog database
        cur.execute("DROP DATABASE IF EXISTS ducklake_catalog")
        cur.execute("CREATE DATABASE ducklake_catalog")

        print("✅ PostgreSQL catalog database created")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"⚠️  PostgreSQL setup failed: {e}")
        return False


def local_development_phase():
    """Phase 1: Local development with DuckDB catalog."""
    print_section("Phase 1: Local Development")

    catalog_path = "ducklake:local_dev.ducklake"

    # Clean up any existing catalog
    cleanup_ducklake(catalog_path)

    print("👨‍💻 Starting local development with DuckDB catalog...")

    with DuckLakeConnection(catalog_path, "dev") as conn:
        conn.execute("USE dev")

        # Create products table
        conn.execute(
            """
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                category VARCHAR,
                price DECIMAL(10, 2),
                in_stock BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert sample data
        conn.execute(
            """
            INSERT INTO products (id, name, category, price, in_stock) VALUES 
                (1, 'DuckDB T-Shirt', 'Apparel', 29.99, true),
                (2, 'DuckDB Mug', 'Accessories', 14.99, true),
                (3, 'DuckDB Sticker Pack', 'Accessories', 4.99, true),
                (4, 'DuckDB Hoodie', 'Apparel', 59.99, false),
                (5, 'DuckDB Cap', 'Apparel', 24.99, true)
        """
        )

        # Create orders table
        conn.execute(
            """
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                product_id INTEGER,
                quantity INTEGER,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                customer_email VARCHAR
            )
        """
        )

        # Insert some orders
        conn.execute(
            """
            INSERT INTO orders (order_id, product_id, quantity, customer_email) VALUES 
                (1, 1, 2, 'alice@example.com'),
                (2, 2, 1, 'bob@example.com'),
                (3, 3, 5, 'charlie@example.com')
        """
        )

        print("✅ Local development tables created and populated")

        # Show current state
        print_query_result(conn, "SELECT * FROM products ORDER BY id", "Products Table")
        print_query_result(
            conn, "SELECT * FROM orders ORDER BY order_id", "Orders Table"
        )

        # Perform some development work
        print("\n🔧 Simulating development work...")

        # Add a view
        conn.execute(
            """
            CREATE VIEW product_inventory AS
            SELECT 
                p.id,
                p.name,
                p.category,
                p.price,
                p.in_stock,
                COALESCE(SUM(o.quantity), 0) as total_ordered
            FROM products p
            LEFT JOIN orders o ON p.id = o.product_id
            GROUP BY p.id, p.name, p.category, p.price, p.in_stock
        """
        )

        print_query_result(
            conn,
            "SELECT * FROM product_inventory ORDER BY id",
            "Product Inventory View",
        )

        # Get catalog statistics
        catalog_size = os.path.getsize(catalog_path.replace("ducklake:", ""))
        print(f"\n📊 Local catalog size: {format_size(catalog_size)}")

    return catalog_path


def migrate_to_postgres(local_catalog):
    """Phase 2: Migrate to PostgreSQL catalog."""
    print_section("Phase 2: Migration to PostgreSQL")

    if not check_postgres_available():
        print("⚠️  PostgreSQL is not available. Simulating migration...")
        print("   In a real scenario, you would:")
        print("   1. Set up PostgreSQL database")
        print("   2. Use DuckLake's migration tools")
        print("   3. Point your application to the new catalog")
        return None

    if not setup_postgres_catalog():
        return None

    # PostgreSQL connection string
    pg_catalog = "ducklake:postgresql://localhost/ducklake_catalog"

    print("🚀 Migrating from local to PostgreSQL catalog...")

    # Export data from local catalog
    with DuckLakeConnection(local_catalog, "dev") as local_conn:
        local_conn.execute("USE dev")

        # Export products
        products_data = local_conn.execute("SELECT * FROM products").fetchall()
        products_columns = [desc[0] for desc in local_conn.description]

        # Export orders
        orders_data = local_conn.execute("SELECT * FROM orders").fetchall()
        orders_columns = [desc[0] for desc in local_conn.description]

    # Import into PostgreSQL catalog
    with DuckLakeConnection(pg_catalog, "prod") as pg_conn:
        pg_conn.execute("USE prod")

        # Recreate schema
        pg_conn.execute(
            """
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                category VARCHAR,
                price DECIMAL(10, 2),
                in_stock BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        pg_conn.execute(
            """
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                product_id INTEGER,
                quantity INTEGER,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                customer_email VARCHAR
            )
        """
        )

        # Import data
        for row in products_data:
            values = ", ".join(
                [f"'{v}'" if isinstance(v, str) else str(v) for v in row]
            )
            pg_conn.execute(f"INSERT INTO products VALUES ({values})")

        for row in orders_data:
            values = ", ".join(
                [f"'{v}'" if isinstance(v, str) else str(v) for v in row]
            )
            pg_conn.execute(f"INSERT INTO orders VALUES ({values})")

        # Recreate view
        pg_conn.execute(
            """
            CREATE VIEW product_inventory AS
            SELECT 
                p.id,
                p.name,
                p.category,
                p.price,
                p.in_stock,
                COALESCE(SUM(o.quantity), 0) as total_ordered
            FROM products p
            LEFT JOIN orders o ON p.id = o.product_id
            GROUP BY p.id, p.name, p.category, p.price, p.in_stock
        """
        )

        print("✅ Data migrated to PostgreSQL catalog")

        # Verify migration
        print_query_result(
            pg_conn,
            "SELECT * FROM products ORDER BY id",
            "Products in PostgreSQL Catalog",
        )

    return pg_catalog


def demonstrate_catalog_flexibility():
    """Demonstrate working with different catalog backends."""
    print_section("Catalog Backend Flexibility")

    print("DuckLake supports multiple catalog backends:")
    print("  ✅ Local DuckDB (for development)")
    print("  ✅ PostgreSQL (for production)")
    print("  ✅ MySQL (for production)")
    print("  ✅ SQLite (for edge deployments)")
    print("  ✅ MotherDuck (for cloud)")

    print("\n📋 Connection string examples:")
    print("  Local:      ducklake:mydata.ducklake")
    print("  PostgreSQL: ducklake:postgresql://host/database")
    print("  MySQL:      ducklake:mysql://host/database")
    print("  SQLite:     ducklake:sqlite:///path/to/catalog.db")
    print("  MotherDuck: ducklake:motherduck:mydb")


def production_operations(catalog_path):
    """Phase 3: Production operations."""
    print_section("Phase 3: Production Operations")

    if not catalog_path:
        print("⚠️  Skipping production operations (no catalog available)")
        return

    print("🏭 Running production operations...")

    with DuckLakeConnection(catalog_path, "prod") as conn:
        conn.execute("USE prod")

        # Simulate production traffic
        print("\n📈 Simulating production traffic...")

        # New orders
        new_orders = [
            (4, 1, 3, "david@example.com"),
            (5, 2, 2, "eve@example.com"),
            (6, 5, 1, "frank@example.com"),
            (7, 1, 1, "grace@example.com"),
            (8, 3, 10, "henry@example.com"),
        ]

        for order_id, product_id, quantity, email in new_orders:
            conn.execute(
                f"""
                INSERT INTO orders (order_id, product_id, quantity, customer_email)
                VALUES ({order_id}, {product_id}, {quantity}, '{email}')
            """
            )
            print(f"   📦 Order {order_id} processed")
            time.sleep(0.2)  # Simulate processing time

        # Update inventory
        conn.execute("UPDATE products SET in_stock = true WHERE id = 4")
        print("   📦 Restocked DuckDB Hoodie")

        # Show production metrics
        print_query_result(
            conn,
            """
            SELECT 
                category,
                COUNT(*) as product_count,
                SUM(CASE WHEN in_stock THEN 1 ELSE 0 END) as in_stock_count,
                AVG(price) as avg_price
            FROM products
            GROUP BY category
            """,
            "Production Metrics by Category",
        )

        print_query_result(
            conn,
            """
            SELECT 
                COUNT(*) as total_orders,
                COUNT(DISTINCT customer_email) as unique_customers,
                SUM(quantity) as total_items_sold
            FROM orders
            """,
            "Order Statistics",
        )


def demonstrate_multi_environment():
    """Demonstrate using multiple environments simultaneously."""
    print_section("Multi-Environment Support")

    print("🌍 DuckLake supports multiple environments simultaneously...")

    # Create different environments
    environments = {
        "dev": "ducklake:dev_env.ducklake",
        "staging": "ducklake:staging_env.ducklake",
        "prod": "ducklake:prod_env.ducklake",
    }

    # Clean up existing
    for env, catalog in environments.items():
        cleanup_ducklake(catalog)

    # Set up each environment
    for env_name, catalog_path in environments.items():
        print(f"\n🔧 Setting up {env_name} environment...")

        with DuckLakeConnection(catalog_path, env_name) as conn:
            conn.execute(f"USE {env_name}")

            # Create a simple config table
            conn.execute(
                """
                CREATE TABLE config (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR,
                    environment VARCHAR
                )
            """
            )

            # Insert environment-specific config
            conn.execute(
                f"""
                INSERT INTO config VALUES 
                    ('api_endpoint', '{env_name}.api.example.com', '{env_name}'),
                    ('debug_mode', '{"true" if env_name == "dev" else "false"}', '{env_name}'),
                    ('max_connections', '{10 if env_name == "dev" else 100}', '{env_name}')
            """
            )

            print(f"   ✅ {env_name} environment configured")

    # Show all environments
    print("\n📊 Environment Configurations:")
    for env_name, catalog_path in environments.items():
        with DuckLakeConnection(catalog_path, env_name) as conn:
            conn.execute(f"USE {env_name}")
            config = conn.execute("SELECT * FROM config ORDER BY key").fetchall()
            print(f"\n{env_name.upper()}:")
            for key, value, _ in config:
                print(f"  {key}: {value}")

    # Clean up
    for env, catalog in environments.items():
        cleanup_ducklake(catalog)


def main():
    """Run the catalog portability demo."""
    print_section("Demo 5: Catalog Portability", 80)

    # Phase 1: Local Development
    local_catalog = local_development_phase()

    # Phase 2: Migration to PostgreSQL
    pg_catalog = migrate_to_postgres(local_catalog)

    # Phase 3: Production Operations
    production_operations(pg_catalog or local_catalog)

    # Additional demonstrations
    demonstrate_catalog_flexibility()
    demonstrate_multi_environment()

    # Cleanup
    print_section("Cleanup")
    cleanup_ducklake(local_catalog)
    if pg_catalog:
        # Note: In real scenario, you'd clean up PostgreSQL tables
        print("✅ PostgreSQL catalog would be cleaned in production")

    print("\n✅ Demo completed!")
    print(
        "\n💡 Key Takeaway: DuckLake's catalog portability enables seamless development-to-production workflows!"
    )


if __name__ == "__main__":
    main()
