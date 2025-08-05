# producers/commodities_stream_producer/producer.py (VERSÃO ATUALIZADA)

import pandas as pd
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configurações ---
KAFKA_TOPIC = 'commodities_prices'
KAFKA_SERVER = 'kafka:29092'
CSV_PATH = 'data/soja_historico.csv'

def create_kafka_producer():
    """Tenta criar uma instância do Kafka Producer em um loop."""
    print("Tentando criar o produtor Kafka para Commodities...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
            )
            print("✅ Produtor Kafka de Commodities conectado com sucesso!")
            return producer
        except NoBrokersAvailable:
            print(f"Não foi possível conectar ao broker em {KAFKA_SERVER}. Tentando novamente em 10s...")
            time.sleep(10)

def run_commodities_stream_producer():
    producer = create_kafka_producer()
    print("--- Produtor de Stream de Commodities (CSV) Iniciado ---")
    
    while True:
        try:
            df = pd.read_csv(CSV_PATH)
            
            # --- MUDANÇAS AQUI ---
            # 1. Limpa e converte a coluna de data para o formato datetime
            df.columns = df.columns.str.strip().str.lower()
            # Assumindo que a coluna de data no seu CSV se chama 'data'
            df['data'] = pd.to_datetime(df['data'], dayfirst=True)
            
            # 2. Ordena o DataFrame pela data, do mais recente para o mais antigo
            df.sort_values(by='data', ascending=False, inplace=True)
            print("CSV lido e ordenado para enviar do mais recente para o mais antigo.")
            # --- FIM DAS MUDANÇAS ---
            
            for index, row in df.iterrows():
                message = row.to_dict()
                data_do_preco = message.get('data').strftime('%Y-%m-%d')
                
                print(f"Enviando preço de {data_do_preco} para o tópico '{KAFKA_TOPIC}'...")
                producer.send(KAFKA_TOPIC, message)
                producer.flush()
                time.sleep(30)
            
            print("Fim do arquivo CSV. Reiniciando o stream em 1 hora.")
            time.sleep(3600)
        except Exception as e:
            print(f"💥 Erro no loop do produtor de commodities: {e}")
            time.sleep(30)

if __name__ == '__main__':
    run_commodities_stream_producer()