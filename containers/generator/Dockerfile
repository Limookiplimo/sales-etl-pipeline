FROM python:3.11

WORKDIR /app
COPY generator.py data.toml .env /app/
RUN pip install --upgrade pip
RUN pip install psycopg2-binary python-dotenv tomli pytz

CMD ["python", "generator.py"]
