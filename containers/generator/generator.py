import tomli
import pathlib
import random
from datetime import datetime
import pytz
import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()
import time

host = os.environ.get("POSTGRES_HOST")
port = os.environ.get("POSTGRES_PORT")
database = os.environ.get("POSTGRESQL_DB")
user = os.environ.get("POSTGRES_USER")
password = os.environ.get("POSTGRES_PASSWORD")

data_path = pathlib.Path(__file__).parent /"data.toml"
data = tomli.loads(data_path.read_text())
products = data['products']['product']
customers = data['customers']['customer']
order_num = 1
invoice_inv = 0
local_timezone = pytz.timezone("Africa/Nairobi")

def create_orders_table(table_name, columns):
    with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute(f"create table if not exists {table_name}({','.join(columns)})")

def load_orders_table(table_name, orders):
    with psycopg2.connect(host=host, port=port, database=database, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.executemany(f"insert into {table_name} values({','.join(['%s'] * len(orders[0]))})", orders)
            conn.commit()

def generate_order_number():
    global order_num
    order_number = f"ORD{order_num:003d}"
    order_num += 1
    return order_number

def generate_invoice_number():
    global invoice_inv
    invoice_number = f"1{invoice_inv:0005d}"
    invoice_inv += 1
    return invoice_number

def generate_order_products():
    customer = random.choice(customers)
    num_products = random.randint(1, len(products))
    selected_products = random.sample(products, num_products)

    order_data = []
    order_num = generate_order_number()
    invoice_number = generate_invoice_number()

    for product in selected_products:
        quantity = random.randint(1, 5)
        price = product["price"]
        weight = product["weight"]
        total_price = quantity * price
        total_weight = quantity * weight
        current_datetime = datetime.now(local_timezone)
        order_data.append((
            customer["c_name"],
            customer["crm"],
            customer["credit_limit"],
            customer["location"],
            order_num,
            invoice_number,
            current_datetime.date(),
            current_datetime.time(),
            product["p_name"],
            product["p_code"],
            quantity,
            weight,
            total_weight,
            price,
            total_price,
        ))
    return order_data

def load_to_database():
    num_orders = 10
    create_orders_table("transactions", 
                        ["customer_name VARCHAR(255)",
                        "crm VARCHAR(255)",
                        "credit_limit FLOAT",
                        "location VARCHAR(255)",
                        "order_number VARCHAR(255)",
                        "invoice_number INTEGER",
                        "order_date DATE",
                        "order_time TIME",
                        "product_name VARCHAR(255)",
                        "product_code VARCHAR(255)",
                        "quantity INTEGER",
                        "weight FLOAT",
                        "total_weight FLOAT",
                        "price FLOAT",
                        "total_price FLOAT"])
    invoice_data = []
    for _ in range(num_orders):
        order_data = generate_order_products()
        invoice_data.extend(order_data)
        load_orders_table("transactions", invoice_data)

load_to_database()
