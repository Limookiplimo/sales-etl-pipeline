from cassandra.cluster import Cluster
from cassandra import AlreadyExists
import os
from dotenv import load_dotenv
load_dotenv()

cassandra_host = [os.environ.get("CASSANDRA_HOST")]
cassandra_keyspace = os.environ.get("CASSANDRA_KEYSPACE")

def connect_to_cassandra(contact_points):
    cluster = Cluster(contact_points)
    session = cluster.connect()
    return cluster, session

def create_keyspace(session, keyspace):
    keyspace_query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
    """
    try:
        session.execute(keyspace_query)
        session.set_keyspace(keyspace)
        print(f"Keyspace {keyspace} created successfully.")
    except AlreadyExists:
        print(f"Keyspace {keyspace} already exists.")

def create_table(session, table_name, table_query):
    try:
        session.execute(table_query)
        print(f"Table {table_name} created successfully.")
    except AlreadyExists:
        print(f"Table {table_name} already exists.")

def intermediate_storage_processes():
    cluster, session = connect_to_cassandra(cassandra_host)
    create_keyspace(session, cassandra_keyspace)

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
        create_table(session, table_name, table_query)

    session.shutdown()
    cluster.shutdown()

intermediate_storage_processes()