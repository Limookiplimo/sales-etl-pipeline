from google.cloud import bigquery
import os, json
from dotenv import load_dotenv
load_dotenv()
credentials = json.loads(os.environ.get("CREDENTIALS"))
if os.path.exists("credentials.json"):
    pass
else:
    with open("credentials.json", "w") as cred:
        json.dump(credentials, cred)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'credentials.json'

project_id = credentials.get("project_id")
client = bigquery.Client(project=project_id)

sales_fact_schema = [
    bigquery.SchemaField("invoice_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("product_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("total_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("total_weight", "FLOAT", mode="REQUIRED"),
]

time_schema = [
    bigquery.SchemaField("date_timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("order_time", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("day", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("week", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
]

customer_schema = [
    bigquery.SchemaField("crm", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("customer_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("credit_limit", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
]

invoice_schema = [
    bigquery.SchemaField("invoice_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("crm", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("total_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("payment_received", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("balance", "FLOAT", mode="NULLABLE"),
]

logistics_schema = [
    bigquery.SchemaField("invoice_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("crm", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("total_weight", "FLOAT", mode="REQUIRED"),
]

product_schema = [
    bigquery.SchemaField("product_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("weight", "FLOAT", mode="REQUIRED"),
]

inventory_track_schema = [
    bigquery.SchemaField("product_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("total_quantity", "INTEGER", mode="REQUIRED"),
]

client.create_table(bigquery.Table(f"{project_id}.sales_etl.sales_fact", schema=sales_fact_schema))
client.create_table(bigquery.Table(f"{project_id}.sales_etl.time_dimension", schema=time_schema))
client.create_table(bigquery.Table(f"{project_id}.sales_etl.customer_dimension", schema=customer_schema))
client.create_table(bigquery.Table(f"{project_id}.sales_etl.invoice_dimension", schema=invoice_schema))
client.create_table(bigquery.Table(f"{project_id}.sales_etl.logistics_dimension", schema=logistics_schema))
client.create_table(bigquery.Table(f"{project_id}.sales_etl.product_dimension", schema=product_schema))
client.create_table(bigquery.Table(f"{project_id}.sales_etl.inventory_track_dimension", schema=inventory_track_schema))

