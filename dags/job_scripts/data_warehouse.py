from google.cloud import bigquery
import os
import json
from dotenv import load_dotenv
load_dotenv()

credentials = json.loads(os.environ.get("CREDENTIALS"))
project_id = credentials.get("project_id")
data_set = os.environ.get("DATASET")

if os.path.exists("credentials.json"):
    pass
else:
    with open("credentials.json", "w") as cred:
        json.dump(credentials, cred)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

client = bigquery.Client(project=project_id)

sales_fact_schema = [
    bigquery.SchemaField("invoice_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("product_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("total_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("total_weight", "FLOAT", mode="REQUIRED"),]
time_schema = [
    bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("day", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("month", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("order_time", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("week", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),]
customer_schema = [
    bigquery.SchemaField("crm", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("credit_limit", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("customer_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("location", "STRING", mode="REQUIRED"),]
invoice_schema = [
    bigquery.SchemaField("invoice_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("crm", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("total_amount", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("payment_received", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("balance", "FLOAT", mode="NULLABLE"),]
logistics_schema = [
    bigquery.SchemaField("invoice_number", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("crm", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("total_weight", "FLOAT", mode="REQUIRED"),]
product_schema = [
    bigquery.SchemaField("product_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("product_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("weight", "FLOAT", mode="REQUIRED"),]
inventory_track_schema = [
    bigquery.SchemaField("product_code", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("order_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("total_quantity", "INTEGER", mode="REQUIRED"),]

schemas = {
    "sales_fact": sales_fact_schema,
    "time_dimension": time_schema,
    "customer_dimension": customer_schema,
    "invoice_dimension": invoice_schema,
    "logistics_dimension": logistics_schema,
    "product_dimension": product_schema,
    "inventory_track_dimension": inventory_track_schema,
}

def check_if_table_exist(client, project_id, dataset, table_name):
    table_ref = client.dataset(dataset).table(table_name)
    try:
        client.get_table(table_ref)
        return True
    except:
        return False

def create_table(client, project_id, dataset, table_name, schema):
    if check_if_table_exist(client, project_id, dataset, table_name):
        print(f"Table {table_name} already exists")
    else:
        table_ref = client.create_table(bigquery.Table(f"{project_id}.{dataset}.{table_name}", schema=schema))
        print(f"Table {table_ref.table_id} created")

def data_warehouse_process():
    for table_name, schema in schemas.items():
        create_table(client, project_id, data_set, table_name, schema)

data_warehouse_process()