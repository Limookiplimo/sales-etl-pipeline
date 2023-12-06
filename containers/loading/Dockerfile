FROM apache/spark:3.5.0

# Run installation tasks as root
USER 0

WORKDIR /app
COPY loading.py .env /app/
RUN pip install google-cloud-bigquery python-dotenv pyspark

ENV PYSPARK_PYTHON python3
CMD ["spark-submit", "--packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0", "loading.py"]
