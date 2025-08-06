# dashboard/app.py (VERSÃO COM FILTRO DE ESTADO)
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# --- Configurações de Conexão com o Data Warehouse (sem alteração) ---
DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = "dw_user", "dw_password", "postgres-dw", "5432", "dw_agro"
CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
GOLD_TABLE = "gold_producao_mensal"

@st.cache_data(ttl=300)
def load_data():
    """Lê os dados da tabela Gold do Data Warehouse (PostgreSQL)."""
    try:
        engine = create_engine(CONN_STRING)
        df = pd.read_sql(f"SELECT * FROM {GOLD_TABLE} ORDER BY ano, mes", engine)
        df['data'] = pd.to_datetime(df['ano'].astype(str) + '-' + df['mes'].astype(str))
        return df
    except Exception as e:
        st.error(f"Erro ao conectar ou ler o Data Warehouse: {e}")
        return pd.DataFrame()

# --- Layout do Dashboard ---
st.set_page_config(page_title="Dashboard de Produção Agrícola", layout="wide")
st.title("🌾 Dashboard de Produção Agrícola Agregada")

df = load_data()

if not df.empty:
    # --- MUDANÇA AQUI: Adicionando o seletor de estado ---
    st.sidebar.header("Filtros")
    # Pega a lista de estados únicos do DataFrame e adiciona a opção 'Todos'
    lista_estados = ['Todos'] + sorted(df['estado'].unique().tolist())
    
    estado_selecionado = st.sidebar.selectbox(
        'Selecione um Estado:',
        lista_estados
    )

    # Filtra o DataFrame com base na seleção
    if estado_selecionado == 'Todos':
        df_filtrado = df.groupby('data')['producao_mensal_total'].sum().reset_index()
        titulo = "Produção Mensal Total (Brasil)"
    else:
        df_filtrado = df[df['estado'] == estado_selecionado]
        titulo = f"Produção Mensal Total ({estado_selecionado})"
    # --- FIM DA MUDANÇA ---
    
    st.subheader(titulo)
    st.bar_chart(df_filtrado.rename(columns={'data': 'index'}).set_index('index'), y='producao_mensal_total')

    st.subheader("Dados Detalhados da Camada Gold")
    st.dataframe(df_filtrado)
else:
    st.warning("Nenhum dado encontrado na camada Gold. Execute a DAG 'build_gold_layer_daily' no Airflow.")