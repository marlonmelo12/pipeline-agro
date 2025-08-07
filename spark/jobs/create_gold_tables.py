from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, month, year

def main():
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    POSTGRES_DW_URL = "jdbc:postgresql://postgres-dw:5432/dw_agro"
    POSTGRES_DW_PROPERTIES = {
        "user": "dw_user", "password": "dw_password", "driver": "org.postgresql.Driver"
    }
    SILVER_PRODUCTION_PATH = "s3a://production-silver/production_data"
    GOLD_PATH = "s3a://gold-layer/producao_mensal_por_estado"

    spark = (
        SparkSession.builder.appName("SilverToGoldAggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.5.0")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    
    df_production = spark.read.format("parquet").load(SILVER_PRODUCTION_PATH)

    print("Aplicando agregações de negócio (produção mensal POR ESTADO)...")
    df_gold = (
        df_production
        .withColumn("ano", year(col("data")))
        .withColumn("mes", month(col("data")))
        .groupBy("ano", "mes", "estado")
        .agg(
            _sum("producao").alias("producao_mensal_total")
        )
        .orderBy("ano", "mes", "estado")
    )
    
    print("Tabela Gold agregada:")
    df_gold.show()

    print(f"Salvando tabela Gold em formato Delta Lake em: {GOLD_PATH}")
    df_gold.write.format("delta").mode("overwrite").partitionBy("estado").save(GOLD_PATH)

    GOLD_TABLE_DW = "gold_producao_mensal"
    print(f"Carregando tabela Gold para o Data Warehouse PostgreSQL na tabela: {GOLD_TABLE_DW}")
    df_gold.write.jdbc(url=POSTGRES_DW_URL, table=GOLD_TABLE_DW, mode="overwrite", properties=POSTGRES_DW_PROPERTIES)

    print("Job da camada Gold concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()