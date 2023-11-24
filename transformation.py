from google.cloud import bigquery
import os, json, shutil
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


from pyspark.sql.functions import from_json, col, sum, from_unixtime, minute, hour, dayofmonth, weekofyear,month, year
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType

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

def schema_define():
    return StructType([
        StructField('customer_name', StringType(), True),
        StructField('crm', StringType(), True),
        StructField('credit_limit', FloatType(), True),
        StructField('location', StringType(), True),
        StructField('order_number', StringType(), True),
        StructField('invoice_number', IntegerType(), True),
        StructField('date', IntegerType(), True),
        StructField('product_name', StringType(),True), 
        StructField('product_code', StringType(), True),
        StructField('quantity', IntegerType(), True),
        StructField('weight', FloatType(), True),
        StructField('total_weight', FloatType(), True),
        StructField('price', FloatType(), True),
        StructField('total_price', FloatType(), True)])

def apply_schema(df, schema):
    return df.select(from_json(col("value"), schema).alias("data")).select("data.*")

def write_to_temporary_storage(df, temp_storage_path):
    df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", temp_storage_path) \
        .option("checkpointLocation", f"./{temp_storage_path}_checkpoint") \
        .start()

def read_from_temporary_storage(spark, temp_storage_path):
    return spark.read.parquet(temp_storage_path)

def write_to_bigquery(df, table_name, project_id, dataset_id):
    df.writeStream \
        .outputMode("update") \
        .format("bigquery") \
        .option("table", f"{project_id}:{dataset_id}.{table_name}") \
        .option("checkpointLocation", f"./{table_name}_checkpoint") \
        .start()\
        .awaitTermination()

def sales_fact(df):
    return df.groupBy("invoice_number", "product_code", "date_timestamp") \
        .agg(sum("total_price").alias("total_amount"),
             sum("total_weight").alias("total_weight"))\
        .withWatermark('date_timestamp', '1 minutes')

def time_dim(df):
    return df.select("date_timestamp", "hour", "minute", "day", "week", "month", "year").distinct()

def customer_dim(df):
    return df.select("crm", "customer_name", "credit_limit", "location").distinct()

def invoice_dim(df):
    return df.groupBy("invoice_number", "date_timestamp", "crm") \
        .agg(sum("total_price").alias("total_amount"))\
        .withWatermark('date_timestamp', '1 minutes')

def logistics_dim(df):
    return df.select("invoice_number", "crm", "location").distinct()

def product_dim(df):
    return df.select("product_name","product_code", "weight").distinct()

def inventory_track_dim(df):
    return df.groupBy("product_code", "date_timestamp") \
        .agg(sum("quantity").alias("total_quantity"))\
        .withWatermark('date_timestamp', '1 minutes')

def main_transformations():
    spark = create_spark_session()
    df = read_kafka_data(spark, "localhost:29092", "postgres.public.transactions")
    parsed_data = parse_kafka_data(df)
    schema = schema_define()
    df_with_schema = apply_schema(parsed_data, schema)
    df_with_schema = df_with_schema.withColumn("date_timestamp", from_unixtime(col("date") * 1000000).cast("Timestamp"))
    df_with_schema = df_with_schema.withColumn("hour", hour(df_with_schema.date_timestamp)) \
                                   .withColumn("minute", minute(df_with_schema.date_timestamp)) \
                                   .withColumn("day", dayofmonth(df_with_schema.date_timestamp)) \
                                   .withColumn("week", weekofyear(df_with_schema.date_timestamp)) \
                                   .withColumn("month", month(df_with_schema.date_timestamp)) \
                                   .withColumn("year", year(df_with_schema.date_timestamp))
    
    temp_storage_path = "./temporary_storage"
    write_to_temporary_storage(df_with_schema, temp_storage_path)
    batch_data = read_from_temporary_storage(spark, temp_storage_path)

    sales_df = sales_fact(batch_data)
    time_df = time_dim(batch_data)
    customer_df = customer_dim(batch_data)
    invoice_df = invoice_dim(batch_data)
    logistics_df = logistics_dim(batch_data)
    product_df = product_dim(batch_data)
    inventory_df = inventory_track_dim(batch_data)

    sales_df.printSchema()
    sales_df.show(5)

    shutil.rmtree(temp_storage_path)

if __name__== "__main__":
    main_transformations()
