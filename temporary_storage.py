from cassandra.cluster import Cluster

# Connect to Cassandra
cluster = Cluster(['172.20.0.2'])
session = cluster.connect()

# Create keyspace
keyspace_query = """
    CREATE KEYSPACE IF NOT EXISTS sales_etl_keyspace
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
"""
session.execute(keyspace_query)
session.set_keyspace('sales_etl_keyspace')

# Create tables
tables = [
    ("sales", """
        CREATE TABLE IF NOT EXISTS sales (
            invoice_number INT PRIMARY KEY,
            product_code TEXT,
            order_date DATE,
            total_amount FLOAT,
            total_weight FLOAT
        )
    """),

    ("time", """
        CREATE TABLE IF NOT EXISTS time (
            order_date DATE PRIMARY KEY,
            order_time TEXT,
            day INT,
            week INT,
            month INT,
            year INT
        )
    """),

    ("customer", """
        CREATE TABLE IF NOT EXISTS customer (
            crm TEXT PRIMARY KEY,
            customer_name TEXT,
            credit_limit FLOAT,
            location TEXT
        )
    """),

    ("invoices", """
        CREATE TABLE IF NOT EXISTS invoices (
            invoice_number INT PRIMARY KEY,
            order_date DATE,
            crm TEXT,
            total_amount FLOAT
        )
    """),

    ("logistics", """
        CREATE TABLE IF NOT EXISTS logistics (
            invoice_number INT,
            crm TEXT,
            location TEXT,
            total_weight FLOAT,
            PRIMARY KEY (invoice_number, crm)
        )
    """),

    ("products", """
        CREATE TABLE IF NOT EXISTS products (
            product_code TEXT PRIMARY KEY,
            product_name TEXT,
            weight FLOAT
        )
    """),

    ("inventory_track", """
        CREATE TABLE IF NOT EXISTS inventory_track (
            product_code TEXT,
            order_date DATE,
            total_quantity INT,
            PRIMARY KEY (product_code, order_date)
        )
    """)
]

for table_name, table_query in tables:
    session.execute(table_query)

session.shutdown()
cluster.shutdown()
