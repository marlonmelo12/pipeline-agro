# producers/sql_stream_producer/producer.py
import pandas as pd
import json
import time
from kafka import KafkaProducer
from sqlalchemy import create_engine

KAFKA_TOPIC = 'production_data'
KAFKA_SERVER = 'kafka:29092'
DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = "agro_user", "agro_password", "postgres-db", "5432", "agro_db"
TABLE_NAME = "production_data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
)
conn_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_string)

print("--- Produtor de Stream de Produção (PostgreSQL) Iniciado ---")
while True:
    try:
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", engine)
        for record in df.to_dict('records'):
            print(f"Enviando dado de produção para o tópico '{KAFKA_TOPIC}'...")
            producer.send(KAFKA_TOPIC, record)
            producer.flush()
            time.sleep(60)
        print("Fim dos dados. Reiniciando a consulta em 1 hora.")
        time.sleep(3600)
    except Exception as e:
        print(f"Erro no produtor SQL: {e}")
        time.sleep(30)