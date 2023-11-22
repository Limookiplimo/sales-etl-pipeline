## Sales Data Pipeline

### Objective
Extract stream data into a Kafka topic from source table in PostgreSQL. Transform the data with spark and load them into a data warehouse on BigQuery. Perform analysis of the data with a desired viz.

### Setup and Configurations
To simulate custom stream data in a retail store, I wrote python scripts to generate random orders with key data points with the help of tomlib. The order details are then sinked into transaction table on postgres which is integrated with debezium as CDC. The debezium connector registers new events into a transactions topic on Kafka every time an order is created.

### Transformation
I chose to process the data with spark stream since KafkaStreams API and Flink did not prove viable for integration with my python scripts. The processed data are loaded into BigQuery warehouse.

### Loading
