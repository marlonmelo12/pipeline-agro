<<<<<<< HEAD

=======
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

<<<<<<< HEAD
=======
# --- Configurações de Conexão ---
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = "dw_user", "dw_password", "postgres-dw", "5432", "dw_agro"
CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Funções de Carregamento de Dados ---
@st.cache_data(ttl=300)
<<<<<<< HEAD
def load_data():
=======
def load_all_data():
    """Lê e junta todas as tabelas da camada Gold do Data Warehouse."""
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    try:
        engine = create_engine(CONN_STRING)
        df_prod = pd.read_sql("SELECT * FROM gold_producao_mensal", engine)
        df_clima = pd.read_sql("SELECT * FROM gold_clima_diario", engine)
        df_precos = pd.read_sql("SELECT * FROM gold_precos_diarios", engine)

        # Prepara as datas para o merge
        for df in [df_prod, df_clima, df_precos]:
            if 'ano' in df.columns:
                df['data'] = pd.to_datetime(df['ano'].astype(str) + '-' + df['mes'].astype(str))
            else:
                df['data'] = pd.to_datetime(df['data'])
        
        # Junta os dados de clima e preço (diários)
        df_merged = pd.merge(df_clima, df_precos, on='data', how='inner')
        return df_prod, df_merged
        
    except Exception as e:
        st.error(f"Erro ao carregar dados do Data Warehouse: {e}")
        return pd.DataFrame(), pd.DataFrame()

<<<<<<< HEAD

st.set_page_config(page_title="Dashboard de Produção Agrícola", layout="wide")
st.title("🌾 Dashboard de Produção Agrícola Agregada")
=======
# --- Layout do Dashboard ---
st.set_page_config(page_title="Dashboard Agro", layout="wide")
st.title("🌾 Dashboard de Análise Agrícola")
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2

df_producao, df_clima_preco = load_all_data()

<<<<<<< HEAD
if not df.empty:
    st.sidebar.header("Filtros")

    lista_estados = ['Todos'] + sorted(df['estado'].unique().tolist())
    
    estado_selecionado = st.sidebar.selectbox(
        'Selecione um Estado:',
        lista_estados
    )

=======
# --- Seção de Produção ---
if not df_producao.empty:
    st.header("Produção Agrícola")
    lista_estados = ['Todos'] + sorted(df_producao['estado'].unique().tolist())
    estado_selecionado = st.selectbox('Selecione um Estado:', lista_estados)
    
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    if estado_selecionado == 'Todos':
        df_filtrado_prod = df_producao.groupby('data')['producao_mensal_total'].sum().reset_index()
        titulo = "Produção Mensal Total (Brasil)"
    else:
        df_filtrado_prod = df_producao[df_producao['estado'] == estado_selecionado]
        titulo = f"Produção Mensal Total ({estado_selecionado})"
    
    st.subheader(titulo)
    st.bar_chart(df_filtrado_prod.rename(columns={'data': 'index'}).set_index('index'), y='producao_mensal_total')

# --- Seção de Clima x Preço ---
if not df_clima_preco.empty:
    st.header("Análise de Clima vs. Preço")
    st.subheader("Gráfico de Dispersão: Precipitação Diária (Média do Estado) vs. Preço da Commodity (USD)")
    
    # Filtro de estado para o gráfico de dispersão
    estado_dispersao = st.selectbox(
        'Selecione um Estado para a Análise de Dispersão:',
        sorted(df_clima_preco['estado'].unique().tolist())
    )
    
    df_filtrado_disp = df_clima_preco[df_clima_preco['estado'] == estado_dispersao]
    
    st.scatter_chart(
        df_filtrado_disp,
        x='precipitacao_media_diaria',
        y='preco_medio_dolar',
        color='#FF4B4B'
    )
    st.caption("Cada ponto representa um dia. O eixo X é a precipitação média em mm no estado e o eixo Y é o preço médio da soja em dólar.")
else:
    st.warning("Dados de produção ou clima/preço não encontrados. Execute a DAG 'build_gold_layer_daily' no Airflow.")