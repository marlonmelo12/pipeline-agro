# spark/Dockerfile (VERSÃO FINAL CORRIGIDA)

FROM bitnami/spark:3.5

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY jobs/ ./jobs/
COPY ./jars/ /opt/bitnami/spark/jars/

# A linha COPY acima já é suficiente, contanto que
# o JAR do Delta Lake e do PostgreSQL estejam na pasta ./jars/
# Exemplo:
# spark/
#   ├── jars/
#   │   ├── delta-spark_2.12-3.1.0.jar
#   │   └── postgresql-42.7.3.jar
#   ├── jobs/
#   └── Dockerfile