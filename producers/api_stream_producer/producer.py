import requests
import json
import time
from kafka import KafkaProducer

KAFKA_TOPIC = 'climate_data'
KAFKA_SERVER = 'kafka:29092' # Usando o nome do serviço e a porta interna
API_URL = 'http://api_mock_service:5001/stream/climate'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("--- Produtor de Stream da API de Clima Iniciado ---")
while True:
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()
            date_str = data[0]['date']
            print(f"Enviando dados de {date_str} para o tópico '{KAFKA_TOPIC}'...")
            producer.send(KAFKA_TOPIC, data)
            producer.flush()
            # Simula um dia a cada 2 minutos
            time.sleep(120) 
        elif response.status_code == 404:
            print("Fim do stream de dados da API. Encerrando o produtor.")
            break
        else:
            print(f"Erro ao chamar API: Status {response.status_code}")
            time.sleep(60) # Espera antes de tentar novamente
    except requests.exceptions.ConnectionError as e:
        print(f"Não foi possível conectar à API de mock... tentando novamente em 30s. Erro: {e}")
        time.sleep(30)