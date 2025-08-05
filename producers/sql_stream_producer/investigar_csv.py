# producers/sql_stream_producer/investigar_csv.py

import pandas as pd
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "data", "dados_producao.csv")

print(f"--- INICIANDO INVESTIGAÇÃO DO ARQUIVO CSV ---")
print(f"Lendo o arquivo: {CSV_FILE_PATH}")

try:
    # Tenta ler as 10 primeiras linhas com a configuração que achamos correta
    df_sample = pd.read_csv(
        CSV_FILE_PATH,
        sep=';',
        header=None,
        encoding='latin-1',
        nrows=10 # Lendo apenas um trecho para análise
    )

    print("\n--- CONTEÚDO BRUTO DAS 10 PRIMEIRAS LINHAS COMO O PANDAS VÊ ---")
    print("Isso nos mostrará a estrutura exata, colunas e conteúdo.")
    print(df_sample)
    
    print("\n--- ANÁLISE DA PRIMEIRA COLUNA (COLUNA 0) ---")
    print("Vamos ver os valores exatos da primeira coluna (onde 'Rondônia' deveria estar):")
    # Imprime cada item entre aspas para vermos espaços em branco ou caracteres ocultos
    for item in df_sample[0].to_list():
        print(f"'{item}'")

except FileNotFoundError:
    print(f"\nERRO: Arquivo não encontrado em '{CSV_FILE_PATH}'. Verifique o caminho.")
except Exception as e:
    print(f"\nOcorreu um erro durante a leitura: {e}")