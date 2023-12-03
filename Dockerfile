FROM python:3.11

WORKDIR /app
COPY generator.py transformation.py loading.py temporary_storage.py data_warehouse.py data.toml requirements.txt /app/
RUN pip install -r requirements.txt

CMD ["python", "temporary_storage.py","data_warehouse.py","generator.py","transformation.py", "loading.py"]