FROM python:3.11

WORKDIR /app
COPY loading.py /app/
RUN pip install pyspark python-dotenv google-cloud-bigquery

CMD ["python", "loading.py"]