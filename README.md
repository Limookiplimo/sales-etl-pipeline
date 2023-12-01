# ETL Pipeline for Stream Data Processing and BigQuery Integration
## Overview
This project demonstrates an ETL pipeline for processing data using Apache Spark and integrating with Google BigQuery. The data is generated using a python script(`generator.py`), processed in real-time with Spark(`transformation.py`), stored in an intermediatestorage database on cassandra and finally loaded into Google BigQuery(`loading.py`).

## Structure Setup
* docker-compose.yaml: Compose file to spin up PostgreSQL, Cassandra, Kafka, Zookeper,Debezium and Schema-registry.
* data.toml: Configuration file containing product and customer data in TOML format
* debezium.json: Debezium connector configuration file for capturing changes from the PostgreSQL database as CDC (Change Data Capture).
* generator.py: Python script to generate synthetic stream data and push it into a PostgreSQL database using psycopg2.
* transformation.py: Apache Spark script to process the streaming data, perform aggregations, and store the results in Cassandra.
* temporary_storage.py: Python script to create necessary tables in Cassandra for intermediate storage.
* data_warehouse.py: Pyhton script to set up data warehouse on Google BigQuery.
* loading.py: Apache Spark script to read data from Cassandra and load it into Google BigQuery.

## Usage
1. Generate and Load Stream Data:
    * Update PostgreSQL database connection details
    * Run `generator.py` to generate and load custom stream data into the PostgreSQL database
2. Capture Changes with Debezium
    * Configure PostgreSQL databse as a source in the Debezium connector usig `debezium.json`.
    * Start the debezium connector to capture changes from source.
3. Transform and Store in Intermediate Storage
    * Update Cassandra connction details, and create necessary tables.
    * Run `transformation` to process streaming data, perform aggregations, and store results in cassandra.
4. Load into BigQuery
    * Set up Google Cloud credentials, and update BigQuery configuration details.
    * Run `loading` to read data from cassandra and load into specified BigQuery tables.

## Environment Variables
Set up sensitive info on the environment variables.

