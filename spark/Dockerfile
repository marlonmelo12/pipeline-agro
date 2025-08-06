# spark/Dockerfile (VERSÃO FINAL)

# Usamos a imagem oficial do Bitnami para Spark, que inclui Python (PySpark)
FROM bitnami/spark:3.5

# Define o diretório de trabalho
WORKDIR /app

# Copia o arquivo de dependências Python (se houver)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia a pasta inteira de jobs para dentro do contêiner
# Agora todos os seus scripts de transformação estarão disponíveis
COPY jobs/ ./jobs/