FROM python:3.11

WORKDIR /app
COPY transformation.py .env /app/
RUN pip install pyspark python-dotenv

CMD ["python", "transformation.py"]
