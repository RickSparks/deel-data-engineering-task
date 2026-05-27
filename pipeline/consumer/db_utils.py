#################################################
#
# 1. Import Libraries
#
import psycopg2
import psycopg2.extras
import os
from datetime import datetime



#################################################
#
# 2. Defining Methods
#

# 2.1 - Get DB connection
def get_connection():
    return psycopg2.connect(
        host=os.getenv("ANALYTICS_DB_HOST", "localhost"),
        port=int(os.getenv("ANALYTICS_DB_PORT", "5433")),
        user=os.getenv("ANALYTICS_DB_USER", "analytics_user"),
        password=os.getenv("ANALYTICS_DB_PASSWORD", "analytics_pass"),
        dbname=os.getenv("ANALYTICS_DB_NAME", "analytics_db"),
    )

# 2.2 - Create RAW schemas and tables
def create_raw_schema(conn):
    
    with conn.cursor() as cur:

        # Creates schema - it will mirror the operations db
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

        # Creates table raw.customers using same structure
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.customers (
                customer_id       BIGINT PRIMARY KEY,
                customer_name     VARCHAR(255),
                is_active         BOOLEAN,
                customer_address  VARCHAR(500),
                updated_at        TIMESTAMP,
                updated_by        BIGINT,
                created_at        TIMESTAMP,
                created_by        BIGINT,
                _op               VARCHAR(1),
                _ingested_at      TIMESTAMP DEFAULT NOW()
            );
        """)

        # Creates table raw.products using same structure
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.products (
                product_id    BIGINT PRIMARY KEY,
                product_name  VARCHAR(255),
                barcode       VARCHAR(100),
                unity_price   DECIMAL(10,2),
                is_active     BOOLEAN,
                updated_at    TIMESTAMP,
                updated_by    BIGINT,
                created_at    TIMESTAMP,
                created_by    BIGINT,
                _op           VARCHAR(1),
                _ingested_at  TIMESTAMP DEFAULT NOW()
            );
        """)

        # Creates table raw.orders using same structure
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.orders (
                order_id       BIGINT PRIMARY KEY,
                order_date     DATE,
                delivery_date  DATE,
                customer_id    BIGINT,
                status         VARCHAR(50),
                updated_at     TIMESTAMP,
                updated_by     BIGINT,
                created_at     TIMESTAMP,
                created_by     BIGINT,
                _op            VARCHAR(1),
                _ingested_at   TIMESTAMP DEFAULT NOW()
            );
        """)

        # Creates table raw.order_items using same structure
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.order_items (
                order_item_id  BIGINT PRIMARY KEY,
                order_id       BIGINT,
                product_id     BIGINT,
                quantity       INTEGER,
                updated_at     TIMESTAMP,
                updated_by     BIGINT,
                created_at     TIMESTAMP,
                created_by     BIGINT,
                _op            VARCHAR(1),
                _ingested_at   TIMESTAMP DEFAULT NOW()
            );
        """)

    conn.commit()
    print(f"[{datetime.now()}] Raw schema and tables ready.")


# 2.3 - Create Upsert for raw.customers table
def upsert_customer(conn, data: dict, op: str):

    # Defining the SQL upsert statement
    sql = """
        INSERT INTO raw.customers (
            customer_id, customer_name, is_active, customer_address,
            updated_at, updated_by, created_at, created_by, _op, _ingested_at
        ) VALUES (
            %(customer_id)s, %(customer_name)s, %(is_active)s, %(customer_address)s,
            %(updated_at)s, %(updated_by)s, %(created_at)s, %(created_by)s,
            %(op)s, NOW()
        )
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_name    = EXCLUDED.customer_name,
            is_active        = EXCLUDED.is_active,
            customer_address = EXCLUDED.customer_address,
            updated_at       = EXCLUDED.updated_at,
            updated_by       = EXCLUDED.updated_by,
            _op              = EXCLUDED._op,
            _ingested_at     = EXCLUDED._ingested_at;
    """

    # Iterate over the data structure to be upserted
    with conn.cursor() as cur:
        cur.execute(sql, {**data, "op": op})
    conn.commit()
    
    print(f"[{datetime.now()}] Upsert to customers table completed.")


# 2.4 - Create Upsert for raw.products table
def upsert_product(conn, data: dict, op: str):

    # Defining the SQL upsert statement
    sql = """
        INSERT INTO raw.products (
            product_id, product_name, barcode, unity_price, is_active,
            updated_at, updated_by, created_at, created_by, _op, _ingested_at
        ) VALUES (
            %(product_id)s, %(product_name)s, %(barcode)s, %(unity_price)s, %(is_active)s,
            %(updated_at)s, %(updated_by)s, %(created_at)s, %(created_by)s,
            %(op)s, NOW()
        )
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            barcode      = EXCLUDED.barcode,
            unity_price  = EXCLUDED.unity_price,
            is_active    = EXCLUDED.is_active,
            updated_at   = EXCLUDED.updated_at,
            updated_by   = EXCLUDED.updated_by,
            _op          = EXCLUDED._op,
            _ingested_at = EXCLUDED._ingested_at;
    """

    # Iterate over the data structure to be upserted
    with conn.cursor() as cur:
        cur.execute(sql, {**data, "op": op})
    conn.commit()
    
    print(f"[{datetime.now()}] Upsert to products table completed.")


# 2.5 - Create Upsert for raw.orders table
def upsert_order(conn, data: dict, op: str):

    # Defining the SQL upsert statement
    sql = """
        INSERT INTO raw.orders (
            order_id, order_date, delivery_date, customer_id, status,
            updated_at, updated_by, created_at, created_by, _op, _ingested_at
        ) VALUES (
            %(order_id)s, %(order_date)s, %(delivery_date)s, %(customer_id)s, %(status)s,
            %(updated_at)s, %(updated_by)s, %(created_at)s, %(created_by)s,
            %(op)s, NOW()
        )
        ON CONFLICT (order_id) DO UPDATE SET
            order_date    = EXCLUDED.order_date,
            delivery_date = EXCLUDED.delivery_date,
            customer_id   = EXCLUDED.customer_id,
            status        = EXCLUDED.status,
            updated_at    = EXCLUDED.updated_at,
            updated_by    = EXCLUDED.updated_by,
            _op           = EXCLUDED._op,
            _ingested_at  = EXCLUDED._ingested_at;
    """

    # Iterate over the data structure to be upserted
    with conn.cursor() as cur:
        cur.execute(sql, {**data, "op": op})
    conn.commit()

    print(f"[{datetime.now()}] Upsert to orders table completed.")



# 2.5 - Create Upsert for raw.order_items table
def upsert_order_item(conn, data: dict, op: str):

    # Defining the SQL upsert statement
    sql = """
        INSERT INTO raw.order_items (
            order_item_id, order_id, product_id, quantity,
            updated_at, updated_by, created_at, created_by, _op, _ingested_at
        ) VALUES (
            %(order_item_id)s, %(order_id)s, %(product_id)s, %(quantity)s,
            %(updated_at)s, %(updated_by)s, %(created_at)s, %(created_by)s,
            %(op)s, NOW()
        )
        ON CONFLICT (order_item_id) DO UPDATE SET
            order_id      = EXCLUDED.order_id,
            product_id    = EXCLUDED.product_id,
            quantity      = EXCLUDED.quantity,
            updated_at    = EXCLUDED.updated_at,
            updated_by    = EXCLUDED.updated_by,
            _op           = EXCLUDED._op,
            _ingested_at  = EXCLUDED._ingested_at;
    """

    # Iterate over the data structure to be upserted
    with conn.cursor() as cur:
        cur.execute(sql, {**data, "op": op})
    conn.commit()

    print(f"[{datetime.now()}] Upsert to order_items table completed.")