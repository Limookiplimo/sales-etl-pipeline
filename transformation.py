from confluent_kafka import Consumer, KafkaError
from collections import defaultdict
from datetime import date, timedelta
import psycopg2
from psycopg2 import sql
import json


def connect_kafka_consumer():
    kafka_config = {
        'bootstrap.servers': '172.19.0.6:29092',
        'group.id': 'invoice-group',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(kafka_config)

def connect_postgres():
    with psycopg2.connect(host="localhost", port=5432, database="etl_pipeline", user="user", password="password") as conn:
        return conn

def load_customer_features(cursor, customer_data):
    #name, crm, location, credit_limit
    query = sql.SQL("insert into customers (customer_name, crm, credit_limit, location) values ({},{},{},{});").format(
        sql.Literal(customer_data['customer_name']),
        sql.Literal(customer_data['crm']),
        sql.Literal(customer_data['credit_limit']),
        sql.Literal(customer_data['location'])
    )
    cursor.execute(query)

def load_sales_features(cursor, sales_data, invoice_totals):
    #inv, date, amt, crm
    date_value = date(1970, 1, 1) + timedelta(days=sales_data['date'])
    query = sql.SQL("insert into sales (invoice_number, date, total_amount, crm) values ({},{},{},{});").format(
        sql.Literal(sales_data['invoice_number']),
        sql.Literal(date_value),
        sql.Literal(invoice_totals[sales_data['invoice_number']]),
        sql.Literal(sales_data['crm'])
    )
    cursor.execute(query)

def load_account_features(cursor, accounts_data, invoice_totals):
    #inv, inv_date, amount, payment, balance
    date_value = date(1970, 1, 1) + timedelta(days=accounts_data['date'])
    query = sql.SQL("insert into accounts (invoice_number, date, total_amount) values ({},{},{});").format(
        sql.Literal(accounts_data['invoice_number']),
        sql.Literal(date_value),
        sql.Literal(invoice_totals[accounts_data['invoice_number']])
    )
    cursor.execute(query)

def load_inventory_track(cursor, inventory_data, product_counts):
    #date, product_code, quantity
    date_value = date(1970, 1, 1) + timedelta(days=inventory_data['date'])
    query = sql.SQL("insert into inventory (product_name, product_code, quantity, date) values({},{},{},{});").format(
        sql.Literal(inventory_data['product_name']),
        sql.Literal(inventory_data['product_code']),
        sql.Literal(product_counts[inventory_data['product_code']]),
        sql.Literal(date_value)
    )
    cursor.execute(query)

def load_logistics_features(cursor, logistic_data, invoice_weights):
    #inv, weight, crm, location
    query = sql.SQL("insert into logistics (invoice_number, total_weight, crm, location) values({},{},{},{});").format(
        sql.Literal(logistic_data['invoice_number']),
        sql.Literal(invoice_weights[logistic_data['invoice_number']]),
        sql.Literal(logistic_data['crm']),
        sql.Literal(logistic_data['location'])
    )
    cursor.execute(query)

def process_kafka_messages(message, cursor, invoice_totals, invoice_weights, product_counts):
    try:
        kafka_data = json.loads(message.value())
        invoice_totals[kafka_data['invoice_number']] += kafka_data['total_price']
        invoice_weights[kafka_data['invoice_number']] += kafka_data['total_weight']
        product_counts[kafka_data['product_code']] += kafka_data['quantity']

        load_customer_features(cursor, kafka_data)
        load_sales_features(cursor, kafka_data, invoice_totals)
        load_account_features(cursor, kafka_data, invoice_totals)
        load_inventory_track(cursor, kafka_data, product_counts)
        load_logistics_features(cursor, kafka_data, invoice_weights)

        cursor.connection.commit()

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except psycopg2.Error as e:
        print(f"Error executing SQL query: {e}")
        cursor.connection.rollback()

def kafka_consumer_loop(consumer, cursor, invoice_totals, invoice_weights, product_counts):
    consumer.subscribe(['c'])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        process_kafka_messages(msg, cursor, invoice_totals, invoice_weights, product_counts)

if __name__ == "__main__":
    kafka_consumer = connect_kafka_consumer()
    postgres_conn = connect_postgres()
    postgres_cursor = postgres_conn.cursor()
    invoice_totals = defaultdict(float)
    invoice_weights = defaultdict(float)
    product_counts = defaultdict(int)

    try:
        kafka_consumer_loop(kafka_consumer, postgres_cursor, invoice_totals, invoice_weights, product_counts)
    except KeyboardInterrupt:
        pass
    finally:
        kafka_consumer.close()
        postgres_cursor.close()
        postgres_conn.close()

