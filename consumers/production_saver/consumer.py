
import json
import io
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio

KAFKA_TOPIC = 'production_data'
MINIO_BUCKET = 'production-raw'

KAFKA_SERVER = 'kafka:29092'
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
<<<<<<< HEAD
    auto_offset_reset='earliest',
=======
    auto_offset_reset='earliest', 
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    group_id=f'{KAFKA_TOPIC}-minio-savers'
)
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)
<<<<<<< HEAD

=======
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
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
<<<<<<< HEAD

=======
        
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
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