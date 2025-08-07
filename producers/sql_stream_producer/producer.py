import pandas as pd
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from sqlalchemy import create_engine


KAFKA_TOPIC = 'production_data'
KAFKA_SERVER = 'kafka:29092'
DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = "agro_user", "agro_password", "postgres-db", "5432", "agro_db"
TABLE_NAME = "production_data"

def create_kafka_producer():
    print("Tentando criar o produtor Kafka para Produção SQL...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            print("Produtor Kafka de Produção SQL conectado com sucesso!")
            return producer
        except NoBrokersAvailable:
            print(f"Não foi possível conectar ao broker em {KAFKA_SERVER}. Tentando novamente em 10s...")
            time.sleep(10)

def run_sql_stream_producer():
    producer = create_kafka_producer()
    conn_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_string)
    
    print("--- Produtor de Stream de Produção (PostgreSQL) Iniciado ---")
    while True:
        try:
            
            df = pd.read_sql(f"SELECT * FROM {TABLE_NAME} ORDER BY data", engine)
            df['data'] = pd.to_datetime(df['data'])
            
        
            grouped_by_date = df.groupby('data')

    
            for date, group_df in grouped_by_date:
                
                message_payload = group_df.to_dict('records')
                
                date_str = date.strftime('%Y-%m-%d')
                num_states = len(message_payload)

                print(f"Enviando dados de {date_str} para {num_states} estado(s) ao tópico '{KAFKA_TOPIC}'...")
                
            
                producer.send(KAFKA_TOPIC, message_payload)
                producer.flush()
                
                time.sleep(10)

            print("Fim dos dados da tabela. Reiniciando a consulta em 1 hora.")
            time.sleep(3600)
        except Exception as e:
            print(f"Erro no produtor SQL: {e}")
            time.sleep(30)

if __name__ == '__main__':
    run_sql_stream_producer()