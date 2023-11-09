import psycopg2
from confluent_kafka import Producer


def extract_sales_data():
    # Database interaction
    with psycopg2.connect(host="localhost", port=5432, database="etl_pipeline", user="user", password="password") as conn:
        with conn.cursor() as cur:
            cur.execute(f"select * from invoices")
            invoices = cur.fetchall()

            # Kafka configuration
            conf = {'bootstrap.servers': '172.21.0.2','client.id': 'python-producer'}
            producer = Producer(conf)
            kafka_topic = "extracts"

            for invoice in invoices:
                key = str(invoice[0])
                value = ','.join(map(str, invoice))
                producer.produce(kafka_topic, key=key, value=value)
                producer.flush()
                    
                print(f"Data loaded to {kafka_topic} successfully.")



