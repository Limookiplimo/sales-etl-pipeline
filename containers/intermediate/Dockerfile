FROM python:3.11

WORKDIR /app
COPY temporary_storage.py .env /app/
RUN pip install cassandra-driver python-dotenv

CMD ["python", "temporary_storage.py"]
