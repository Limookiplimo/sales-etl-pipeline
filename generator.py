import tomli
import pathlib
import random
from datetime import datetime
import psycopg2

data_path = pathlib.Path(__file__).parent /"data.toml"
data = tomli.loads(data_path.read_text())
products = data['products']['product']
customers = data['customers']['customer']
order_num = 1
invoice_inv = 0

def create_orders_table(table_name, columns):
    with psycopg2.connect(host="localhost", port=5432, database="etl_pipeline", user="user", password="password") as conn:
        with conn.cursor() as cur:
            cur.execute(f"create table if not exists {table_name}({','.join(columns)})")

def load_orders_table(table_name, orders):
    with psycopg2.connect(host="localhost", port=5432, database="etl_pipeline", user="user", password="password") as conn:
        with conn.cursor() as cur:
            cur.executemany(f"insert into {table_name} values({','.join(['%s'] * len(orders[0]))})", orders)
            conn.commit()

def get_item_count():
    with psycopg2.connect(host="localhost", port=5432, database="etl_pipeline", user="user", password="password") as conn:
        with conn.cursor() as cur:
            cur.execute(f"select count(*) from invoices")
            count = cur.fetchone()[0]
    return count

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
    line_num = get_item_count() + 1
    line_number = int(line_num)
    invoice_number = generate_invoice_number()

    for product in selected_products:
        quantity = random.randint(1, 5)
        price = product["price"]
        weight = product["weight"]
        total_price = quantity * price
        total_weight = quantity * weight
        order_data.append((
            customer["c_name"],
            customer["crm"],
            customer["credit_limit"],
            customer["location"],
            order_num,
            invoice_number,
            line_number,
            datetime.now(),
            product["p_name"],
            product["p_code"],
            quantity,
            weight,
            total_weight,
            price,
            total_price,

        ))
        line_number += 1
        
    return order_data

if __name__ == "__main__":
    num_orders = 3
    create_orders_table("invoices", ["customer_name VARCHAR(255)","crm VARCHAR(255)","credit_limit FLOAT","location VARCHAR(255)","order_number VARCHAR(255)","invoice_number INTEGER","line_number INTEGER","date DATE","product_name VARCHAR(255)", "product_code VARCHAR(255)","quantity INTEGER","weight FLOAT","total_weight FLOAT","price FLOAT","total_price FLOAT"])
    
    invoice_data = []
    for _ in range(num_orders):
        order_data = generate_order_products()
        invoice_data.extend(order_data)
    
    load_orders_table("invoices", invoice_data)

