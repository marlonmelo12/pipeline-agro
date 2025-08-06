import pandas as pd
from sqlalchemy import create_engine
import os
import locale

# Tenta usar o locale do sistema para português
try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
except locale.Error:
    print("⚠️ Locale pt_BR.utf8 não está disponível. Datas podem falhar ao converter.")

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
    print(f"🔄 Iniciando carga de dados do CSV: {CSV_FILE_PATH}...")

    try:
        # Leitura completa do CSV
        df = pd.read_csv(CSV_FILE_PATH)

        # Verificações básicas
        if df.empty:
            raise ValueError("❌ O arquivo CSV está vazio.")

        expected_columns = {"data", "unidade_federativa", "producao"}
        if not expected_columns.issubset(df.columns):
            raise ValueError(f"❌ O CSV deve conter as colunas: {expected_columns}")

        # Limpa dados nulos
        df = df.dropna(subset=['data', 'unidade_federativa', 'producao'])

        # Converte produção para numérico
        df['producao'] = pd.to_numeric(df['producao'], errors='coerce')
        df = df.dropna(subset=['producao'])

        # Converte data de formato "jan/15" para datetime (mês em português)
        month_map = {
            'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04', 'mai': '05', 'jun': '06',
            'jul': '07', 'ago': '08', 'set': '09', 'out': '10', 'nov': '11', 'dez': '12'
        }

        def convert_date(data_str):
            try:
                mes_abrev, ano = data_str.lower().split('/')
                mes_num = month_map.get(mes_abrev)
                if mes_num:
                    return f"20{ano}-{mes_num}-01"  # dia fixo como 01
            except:
                return None

        df['data'] = df['data'].apply(convert_date)
        df = df.dropna(subset=['data'])

        # Conecta ao banco e insere
        conn_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(conn_string)

        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)

        print(f"✅ Sucesso! Inseridos {len(df)} registros na tabela '{TABLE_NAME}'.")
        print(df.head())

    except Exception as e:
        print(f"❌ ERRO durante o processamento: {e}")

if __name__ == "__main__":
    setup_database_from_final_csv()
