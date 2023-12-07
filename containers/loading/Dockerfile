FROM apache/spark:3.5.0

USER 0

WORKDIR /app
COPY loading.py credentials.json .env /app/
RUN pip install google-cloud-bigquery python-dotenv pyspark

ENV PYSPARK_PYTHON python3
ENV GOOGLE_APPLICATION_CREDENTIALS /app/credentials.json

CMD ["spark-submit", "--packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0", "loading.py"]
