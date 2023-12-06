FROM python:3.11

WORKDIR /app
COPY data_warehouse.py .env /app/
RUN pip install google-cloud-bigquery python-dotenv

CMD ["python", "data_warehouse.py"]
