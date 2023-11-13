import psycopg2
from confluent_kafka import Producer
import time


def extract_sales_data():
    # Database interaction
    with psycopg2.connect(host="localhost", port=5432, database="etl_pipeline", user="user", password="password") as conn:
        with conn.cursor() as cur:
            cur.execute(f"select * from invoices")
            invoices = cur.fetchall()
            # Kafka configuration
            conf = {'bootstrap.servers': '0.0.0.0:9092',}
            producer = Producer(conf)
            kafka_extracts_topic = "extracts"
            kafka_staging_topic = "staging"
            

            for invoice in invoices:
                key = str(invoice[0])
                value = ','.join(map(str, invoice))
                producer.produce(kafka_extracts_topic, key=key, value=value)
                
                producer.flush()
                    
            print(f"Data loaded to {kafka_extracts_topic} successfully.")

