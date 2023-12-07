from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import FloatType
from google.cloud import bigquery
import os
import json
from dotenv import load_dotenv
load_dotenv()

credentials = json.loads(os.environ.get("CREDENTIALS"))
if os.path.exists("credentials.json"):
    pass
else:
    with open("credentials.json", "w") as cred:
        json.dump(credentials, cred)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'credentials.json'

project_id =  os.environ.get("PROJECT_ID")
client = bigquery.Client(project=project_id)

dataset_id = os.environ.get("DATASET_ID")
project = os.environ.get("PROJECT")
cassandra_host = os.environ.get("CASSANDRA_HOST")
cassandra_port = os.environ.get("CASSANDRA_PORT")
cassandra_keyspace = os.environ.get("CASSANDRA_KEYSPACE")
fact_table =os.environ.get("FACT_TABLE")
customer_dim = os.environ.get("CUSTOMER_DIMENSION")
time_dim = os.environ.get("TIME_DIMENSION")
invoices_dim = os.environ.get("INVOICES_DIMENSION")
logistics_dim = os.environ.get("LOGISTICS_DIMENSION")
inventory_dim = os.environ.get("INVENTORY_TRACK_DIMENSION")
products_dim = os.environ.get("PRODUCTS_DIMENSION")

SALES_TABLE = "sales"
TIME_TABLE = "time"
CUSTOMER_TABLE = "customer"
INVOICES_TABLE = "invoices"
LOGISTICS_TABLE = "logistics"
PRODUCTS_TABLE = "products"
INVENTORY_TABLE = "inventory_track"

def create_spark_session():
    return SparkSession.builder \
        .appName("CassandraToBigQuery") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "./credentials.json") \
        .getOrCreate()

def read_cassandra_table(spark, keyspace, table):
    return spark \
        .read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .load()

def write_to_bigquery(df,table_id):
    df.write \
        .format("bigquery") \
        .option("writeMethod","direct") \
        .option("table", f"{table_id}") \
        .mode("overwrite") \
        .save()
    
def load_cassandra_to_bigquery(spark, cassandra_table, bigquery_table):
    cassandra_df = read_cassandra_table(spark, cassandra_keyspace, cassandra_table)
    if bigquery_table == invoices_dim:
        cassandra_df = cassandra_df \
            .withColumn("payment_received", lit(None).cast(FloatType())) \
            .withColumn("balance", lit(None).cast(FloatType()))
    write_to_bigquery(cassandra_df, bigquery_table)

def main_loading_process():
    spark = create_spark_session()

    load_cassandra_to_bigquery(spark, SALES_TABLE, fact_table)
    load_cassandra_to_bigquery(spark, CUSTOMER_TABLE, customer_dim)
    load_cassandra_to_bigquery(spark, TIME_TABLE, time_dim)
    load_cassandra_to_bigquery(spark, INVOICES_TABLE, invoices_dim)
    load_cassandra_to_bigquery(spark, LOGISTICS_TABLE, logistics_dim)
    load_cassandra_to_bigquery(spark, INVENTORY_TABLE, inventory_dim)
    load_cassandra_to_bigquery(spark, PRODUCTS_TABLE, products_dim)

main_loading_process()