import json
import requests

connector_config = {
    "name": "transaction-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "172.18.0.8",
        "database.port": "5432",
        "database.user": "user",
        "database.password": "password",
        "database.dbname": "etl_pipeline",
        "database.server.name": "postgres",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "table.include.list": "public.transactions",
        "slot.name": "dbz_transaction_slot"
    }
}

debezium_api_url = "http://localhost:8083/connectors"

def create_debezium_connector(config):
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    response = requests.post(debezium_api_url, data=json.dumps(config), headers=headers)
    if response.status_code == 201:
        print("Debezium Connector created successfully.")
    else:
        print(f"Failed to create Debezium Connector. Status Code: {response.status_code}")
        print(response.text)

create_debezium_connector(connector_config)
