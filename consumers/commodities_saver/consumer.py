# consumers/commodities_saver/consumer.py (VERSÃO DE DEPURAÇÃO)

import json
import io
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio

print("--- Módulo consumer.py importado [CHECKPOINT 1] ---")

# --- Configurações ---
KAFKA_TOPIC = 'commodities_prices'
MINIO_BUCKET = 'commodities-raw'
KAFKA_SERVER = 'kafka:29092'
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

def create_kafka_consumer():
    print("--- Dentro de create_kafka_consumer [CHECKPOINT 2] ---")
    consumer = None
    while not consumer:
        try:
            print(f"Tentando conectar o consumidor ao Kafka em {KAFKA_SERVER}...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                auto_offset_reset='earliest',
                group_id=f'{KAFKA_TOPIC}-minio-savers',
                consumer_timeout_ms=10000 # Timeout de 10s para não ficar preso para sempre
            )
            print(f"✅ Consumidor Kafka conectado ao tópico '{KAFKA_TOPIC}'! [CHECKPOINT 3]")
            return consumer
        except NoBrokersAvailable:
            print(f"Broker Kafka não disponível. Tentando novamente em 15s...")
            time.sleep(15)
        except Exception as e:
            print(f"Falha inesperada ao conectar consumer ao Kafka: {e}. Tentando de novo em 15s...")
            time.sleep(15)

def run_saver():
    print("--- Dentro de run_saver [CHECKPOINT 4] ---")
    consumer = create_kafka_consumer()
    
    print("--- Tentando criar cliente MinIO... [CHECKPOINT 5] ---")
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    print("✅ Cliente MinIO criado. [CHECKPOINT 6]")

    # Garante que o bucket existe
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            print(f"Bucket '{MINIO_BUCKET}' criado no MinIO.")
    except Exception as e:
        print(f"Erro ao verificar/criar bucket no MinIO: {e}")
        # Decide se quer parar ou continuar tentando
        return 

    print(f"--- Iniciando loop de escuta de mensagens... [CHECKPOINT 7] ---")
    for message in consumer:
        try:
            print(f"Mensagem recebida do tópico '{KAFKA_TOPIC}'!")
            raw_data_bytes = message.value
            now = datetime.now()
            object_name = f"{now.strftime('%Y/%m/%d')}/{now.strftime('%H-%M-%S')}-{message.offset}.json"
            
            minio_client.put_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                data=io.BytesIO(raw_data_bytes),
                length=len(raw_data_bytes),
                content_type='application/json'
            )
            print(f"Mensagem {message.offset} salva em MinIO: {MINIO_BUCKET}/{object_name}")
            
        except Exception as e:
            print(f"💥 Erro ao processar mensagem: {e}")

    print("--- Loop de mensagens terminado (ou timeout). Reiniciando o processo. ---")
    time.sleep(5)


if __name__ == '__main__':
    print("--- Script consumer.py iniciado (__main__) [CHECKPOINT 0] ---")
    # Loop infinito para garantir que o consumidor sempre tente rodar
    while True:
        run_saver()