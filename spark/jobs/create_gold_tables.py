# spark/jobs/create_gold_tables.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, month, year

def main():
    # --- Configurações ---
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    
    POSTGRES_URL = "jdbc:postgresql://postgres-db:5432/agro_db"
    POSTGRES_PROPERTIES = {
        "user": "agro_user",
        "password": "agro_password",
        "driver": "org.postgresql.Driver"
    }

    SILVER_COMMODITIES_PATH = "s3a://commodities-silver/precos_soja"
    SILVER_PRODUCTION_PATH = "s3a://production-silver/daily_total_production"
    GOLD_PATH = "s3a://gold-layer/producao_mensal_agregada"

    # --- 1. Inicialização da Spark Session com suporte a Delta Lake ---
    spark = (
        SparkSession.builder.appName("SilverToGoldAggregation")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-spark_2.12:2.4.0,org.postgresql:postgresql:42.5.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    print("Spark Session para camada Gold iniciada com suporte a Delta Lake.")

    # --- 2. Leitura dos Dados da Camada Silver ---
    print(f"Lendo dados da camada Silver...")
    df_commodities = spark.read.parquet(SILVER_COMMODITIES_PATH)
    df_production = spark.read.parquet(SILVER_PRODUCTION_PATH)

    # --- 3. Transformação e Agregação de Negócio ---
    print("Aplicando agregações de negócio...")
    # Exemplo: Calcular a produção total e o preço médio mensal por produto e estado
    # (Neste caso, vamos simplificar e agregar a produção total mensal)
    df_gold = (
        df_production
        .withColumn("ano", year(col("data")))
        .withColumn("mes", month(col("data")))
        .groupBy("ano", "mes")
        .agg(
            _sum("producao_total_toneladas").alias("producao_mensal_total")
        )
        .orderBy("ano", "mes")
    )
    
    print("Tabela Gold agregada:")
    df_gold.show()

    # --- 4. Carga na Camada Gold (Delta Lake no MinIO) ---
    print(f"Salvando tabela Gold em formato Delta Lake em: {GOLD_PATH}")
    (
        df_gold.write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )

    # --- 5. Carga no Data Warehouse (PostgreSQL) ---
    GOLD_TABLE_DW = "gold_producao_mensal"
    print(f"Carregando tabela Gold para o Data Warehouse PostgreSQL na tabela: {GOLD_TABLE_DW}")
    (
        df_gold.write
        .jdbc(url=POSTGRES_URL, table=GOLD_TABLE_DW, mode="overwrite", properties=POSTGRES_PROPERTIES)
    )

    print("Job da camada Gold concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()