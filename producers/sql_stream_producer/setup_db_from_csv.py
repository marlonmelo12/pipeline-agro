# producers/sql_stream_producer/setup_db_from_final_csv.py (VERSÃO FINAL COM TRATAMENTO DE ERRO DE DATA)

import pandas as pd
from sqlalchemy import create_engine
import locale
import os

# --- Configurações ---
DB_USER = "agro_user"
DB_PASSWORD = "agro_password"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "agro_db"
TABLE_NAME = "production_data"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "data", "dados_producao.csv")

def setup_database_from_final_csv():
    print(f"INICIANDO CARGA DE DADOS DO CSV FINAL PARA O POSTGRESQL...")
    
    try:
        # --- PASSO 1: Leitura do CSV ---
        print(f"Lendo o arquivo CSV bruto: {CSV_FILE_PATH}...")
        df_wide = pd.read_csv(
            CSV_FILE_PATH,
            sep=',',
            header=[3, 4],
            index_col=0,
            na_values='-',
            encoding='utf-8-sig'
        )

        # --- PASSO 2: Limpeza ---
        df_wide.index.name = 'unidade_federativa'
        if "Fonte: IBGE - Levantamento Sistemático da Produção Agrícola" in df_wide.index:
            df_wide = df_wide.drop("Fonte: IBGE - Levantamento Sistemático da Produção Agrícola")

        # --- PASSO 3: Transformação (Unpivot) ---
        print("Transformando dados...")
        df_stacked = df_wide.stack(level=[0, 1], future_stack=True).reset_index()

        # --- PASSO 4: Limpeza Final ---
        print("Limpando e formatando dados...")
        df_stacked.columns = ['unidade_federativa', 'mes', 'produto', 'producao']
        df_stacked.dropna(subset=['producao'], inplace=True)
        df_stacked['produto'] = df_stacked['produto'].astype(str).str.split(' ').str[1]
        
        try:
            locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
        except locale.Error:
            print("AVISO: Locale 'pt_BR.UTF-8' não encontrado.")
        
        # --- CORREÇÃO FINAL ESTÁ AQUI ---
        # Converte a coluna de data de forma robusta, transformando erros em NaT (Not a Time)
        df_stacked['data'] = pd.to_datetime(df_stacked['mes'], format='%B %Y', errors='coerce')
        
        # Remove as linhas onde a data não pôde ser convertida (ficou NaT)
        df_stacked.dropna(subset=['data'], inplace=True)
        
        # O resto do script continua normalmente
        df_stacked = df_stacked[df_stacked['data'].dt.year >= 2015].copy()
        df_stacked['data'] = df_stacked['data'].dt.strftime('%Y-%m-%d')
        df_tidy = df_stacked[['data', 'unidade_federativa', 'produto', 'producao']].copy()
        
        if df_tidy['producao'].dtype == 'object':
             df_tidy['producao'] = df_tidy['producao'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
        else:
            df_tidy['producao'] = pd.to_numeric(df_tidy['producao'])

        print("Transformação concluída. O formato final dos dados é:")
        print(df_tidy.head())

        # --- PASSO 5: Carga no PostgreSQL ---
        conn_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(conn_string)
        print(f"Inserindo {len(df_tidy)} registros na tabela '{TABLE_NAME}'...")
        df_tidy.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        print(f"\n✅ Sucesso! Tabela '{TABLE_NAME}' populada no PostgreSQL.")

    except Exception as e:
        print(f"💥 ERRO AO CARREGAR DADOS DE PRODUÇÃO: {e}")

if __name__ == "__main__":
    setup_database_from_final_csv()