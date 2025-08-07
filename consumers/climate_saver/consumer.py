# consumers/climate_saver/consumer.py (EXEMPLO)
import json
import io
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio

KAFKA_TOPIC = 'climate_data'
MINIO_BUCKET = 'climate-raw'

KAFKA_SERVER = 'kafka:29092'
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest', 
    group_id=f'{KAFKA_TOPIC}-minio-savers'
)
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

found = minio_client.bucket_exists(MINIO_BUCKET)
if not found:
    minio_client.make_bucket(MINIO_BUCKET)
    print(f"Bucket '{MINIO_BUCKET}' criado no MinIO.")
else:
    print(f"Bucket '{MINIO_BUCKET}' já existe.")

print(f"Ouvindo o tópico '{KAFKA_TOPIC}' para salvar no bucket '{MINIO_BUCKET}'...")
for message in consumer:
    try:
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
        print(f" Erro ao processar mensagem: {e}")