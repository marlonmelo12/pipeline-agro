import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME = "dw_user", "dw_password", "postgres-dw", "5432", "dw_agro"
CONN_STRING = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@st.cache_data(ttl=300)
def load_data():
    engine = create_engine(CONN_STRING)
    clima = pd.read_sql("SELECT * FROM public.gold_precipitacao_diaria", engine)
    soja = pd.read_sql("SELECT * FROM public.gold_precos_soja_diarios", engine)
    return clima, soja

st.set_page_config(page_title="Análise Clima vs Preço da Soja", layout="wide")
st.title("☁️🌱 Relação entre Precipitação e Preço da Soja")

clima_df, soja_df = load_data()

df_merge = pd.merge(clima_df, soja_df, how="inner", on="data")


estados = ['Todos'] + sorted(df_merge['estado'].dropna().unique())
estado_sel = st.sidebar.selectbox("Filtrar por Estado", estados)

data_min, data_max = df_merge['data'].min(), df_merge['data'].max()
data_ini, data_fim = st.sidebar.date_input("Intervalo de datas", [data_min, data_max])

if estado_sel != 'Todos':
    df_merge = df_merge[df_merge['estado'] == estado_sel]

df_merge = df_merge[(df_merge['data'] >= pd.to_datetime(data_ini)) & (df_merge['data'] <= pd.to_datetime(data_fim))]

col1, col2 = st.columns(2)
col1.metric("🌧️ Precipitação Média (mm)", round(df_merge['precipitacao_mm'].mean(), 2))
col2.metric("💰 Preço Médio da Soja", f"R$ {round(df_merge['preco'].mean(), 2)}")

st.plotly_chart(
    px.line(df_merge, x="data", y="precipitacao_mm", title="Precipitação Diária", markers=True),
    use_container_width=True
)

st.plotly_chart(
    px.line(df_merge, x="data", y="preco", title="Preço Diário da Soja", markers=True),
    use_container_width=True
)

st.plotly_chart(
    px.scatter(df_merge, x="precipitacao_mm", y="preco", trendline="ols",
               title="Correlação entre Precipitação e Preço da Soja"),
    use_container_width=True
)
