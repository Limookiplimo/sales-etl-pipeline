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

def create_orders_table(table_name, columns):
    with psycopg2.connect(host="localhost", port=5432, database="etl_pipeline", user="user", password="password") as conn:
        with conn.cursor() as cur:
            cur.execute(f"create table if not exists {table_name}({','.join(columns)})")

def load_orders_table(table_name, orders):
    with psycopg2.connect(host="localhost", port=5432, database="etl_pipeline", user="user", password="password") as conn:
        with conn.cursor() as cur:
            cur.executemany(f"insert into {table_name} values({','.join(['%s'] * len(orders[0]))})", orders)
            conn.commit()

def generate_order_number():
    global order_num
    order_number = f"ORD{order_num:003d}"
    order_num += 1
    return order_number

def generate_order_products():
    customer = random.choice(customers)
    num_products = random.randint(1, len(products))
    selected_products = random.sample(products, num_products)

    order_data = []
    order = {
        "customer_name": customer["c_name"],
        "crm": customer["crm"],
        "order_number": generate_order_number(),
        "date": datetime.now(),
    }
    total_price = 0
    
    for product in selected_products:
        quantity = random.randint(1, 5)
        price = product["price"]
        total_price += quantity * price
        order_data.append((
            order["customer_name"],
            order["crm"],
            order["order_number"],
            order["date"],
            product["p_name"],
            quantity,
            price,
            total_price
        ))
    return order_data

if __name__ == "__main__":
    num_orders = 20
    create_orders_table("invoices", ["customer_name VARCHAR(255)","crm VARCHAR(255)","order_number VARCHAR(255)","date DATE","product_name VARCHAR(255)","quantity INTEGER","price FLOAT","total_price FLOAT"])
    
    invoice_data = []
    for _ in range(num_orders):
        order_data = generate_order_products()
        invoice_data.extend(order_data)
    
    load_orders_table("invoices", invoice_data)



    









