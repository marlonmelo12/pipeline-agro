# spark/Dockerfile

# Usamos uma imagem oficial do PySpark que já vem com tudo configurado
FROM apache/spark-py

# Define o diretório de trabalho
WORKDIR /app

# Copia o nosso script de transformação
COPY transform_commodities_stream.py .

# As dependências de JARs serão baixadas pelo próprio Spark na execução