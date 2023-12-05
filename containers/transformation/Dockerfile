FROM apache/spark:3.5.0

# Run installation tasks as root
USER 0

WORKDIR /app
COPY transformation.py .env /app/
RUN pip install python-dotenv pyspark
EXPOSE 4040
ENV PYSPARK_PYTHON python3
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0", "transformation.py"]
