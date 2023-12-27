FROM apache/airflow:2.8.0

USER root
RUN apt-get update && \
    apt-get install -y wget default-jre && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/ && \
    rm -rf /var/cache/oracle-jdk8-installer && \
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))" && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile
RUN mkdir -p /opt/spark/logs && \
    chmod 755 /opt/spark/logs && \
    chown airflow:airflow /opt/spark/logs
RUN wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.0-bin-hadoop3.tgz && \
    mv spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz    
ENV SPARK_HOME=/opt/spark
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.35.0/spark-bigquery-with-dependencies_2.12-0.35.0.jar
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.2.0/spark-cassandra-connector_2.12-3.2.0.jar
RUN apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install apache-airflow-providers-apache-spark tomli
