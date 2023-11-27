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
dataset_id = os.environ.get("DATASET_ID")
project_id = credentials.get("project_id")
client = bigquery.Client(project=project_id)

from pyspark.sql.functions import from_json, col, sum, from_unixtime, dayofmonth, weekofyear,month, year, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("SalesProcessing") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0")\
        .getOrCreate()
    
def read_kafka_data(spark, bootstrap_servers, topic):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",bootstrap_servers) \
        .option("subscribe",topic) \
        .load()

def parse_kafka_data(df):
    return df.selectExpr("CAST (key AS STRING)", "CAST(value AS STRING)")

def kafka_schema_define():
    return StructType([
        StructField('customer_name', StringType(), True),
        StructField('crm', StringType(), True),
        StructField('credit_limit', FloatType(), True),
        StructField('location', StringType(), True),
        StructField('order_number', StringType(), True),
        StructField('invoice_number', IntegerType(), True),
        StructField('order_date', IntegerType(), True),
        StructField('order_time', IntegerType(), True),
        StructField('product_name', StringType(),True), 
        StructField('product_code', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('weight', FloatType(), True),
        StructField('total_weight', FloatType(), True),
        StructField('price', FloatType(), True),
        StructField('total_price', FloatType(), True)])

def apply_schema(df, schema):
    return df.select(from_json(col("value"), schema).alias("data")).select("data.*")

def sales_fact(df):
    return df.groupBy("invoice_number", "product_code", "order_date") \
        .agg(sum("total_price").alias("total_amount"),
             sum("total_weight").alias("total_weight"))

def time_dim(df):
    return df.select("order_date", "order_time", "day", "week", "month", "year").distinct()

def customer_dim(df):
    return df.select("crm", "customer_name", "credit_limit", "location").distinct()

def invoice_dim(df):
    return df.groupBy("invoice_number", "order_date", "crm") \
        .agg(sum("total_price").alias("total_amount"))

def logistics_dim(df):
    return df.select("invoice_number", "crm", "location").distinct()

def product_dim(df):
    return df.select("product_name","product_code", "weight").distinct()

def inventory_track_dim(df):
    return df.groupBy("product_code", "order_date") \
        .agg(sum("quantity").alias("total_quantity"))
 
def main_transformations():
    spark = create_spark_session()
    df = read_kafka_data(spark, "localhost:29092", "postgres.public.transactions")
    parsed_data = parse_kafka_data(df)
    schema = kafka_schema_define()
    applied_schema = apply_schema(parsed_data, schema)
    df_with_schema = applied_schema \
        .withColumn("order_date", from_unixtime(col("order_date") * 86400).cast("date"))\
        .withColumn("order_time", date_format(from_unixtime(col("order_time") * 100000), "HH:mm:ss"))
    df_with_schema = df_with_schema.withColumn("day", dayofmonth(df_with_schema.order_date)) \
                                   .withColumn("week", weekofyear(df_with_schema.order_date)) \
                                   .withColumn("month", month(df_with_schema.order_date)) \
                                   .withColumn("year", year(df_with_schema.order_date))
    
    sales_df = sales_fact(df_with_schema)
    time_df = time_dim(df_with_schema)
    customer_df = customer_dim(df_with_schema)
    invoice_df = invoice_dim(df_with_schema)
    logistics_df = logistics_dim(df_with_schema)
    product_df = product_dim(df_with_schema)
    inventory_df = inventory_track_dim(df_with_schema)

    sales_query = sales_df.writeStream.outputMode("complete").format("console").start()
    time_query = time_df.writeStream.format("console").start()
    customer_query = customer_df.writeStream.format("console").start()
    invoices_query = invoice_df.writeStream.outputMode("complete").format("console").start()
    logistics_query = logistics_df.writeStream.format("console").start()
    products_query = product_df.writeStream.format("console").start()
    inventory_query = inventory_df.writeStream.outputMode("complete").format("console").start()

    sales_query.awaitTermination()
    time_query.awaitTermination()
    customer_query.awaitTermination()
    invoices_query.awaitTermination()
    logistics_query.awaitTermination()
    products_query.awaitTermination()
    inventory_query.awaitTermination()

main_transformations()