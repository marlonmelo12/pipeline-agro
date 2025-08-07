from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, date_format

def main():
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    POSTGRES_DW_URL = "jdbc:postgresql://postgres-dw:5432/dw_agro"
    POSTGRES_DW_PROPERTIES = {"user": "dw_user", "password": "dw_password", "driver": "org.postgresql.Driver"}
    SILVER_PATH = "s3a://commodities-silver/precos_soja"
    GOLD_PATH = "s3a://gold-layer/precos_diarios_commodities"
    GOLD_TABLE_DW = "gold_precos_diarios"

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

    df_silver = spark.read.format("parquet").load(SILVER_PATH)

    # Converte timestamp para data e calcula o preço médio do dia
    df_gold = (
        df_silver
        .withColumn("data", col("timestamp").cast("date"))
        .groupBy("data")
        .agg(avg("preco").alias("preco_medio_dolar"))
        .orderBy("data")
    )

    print("Tabela Gold de Preços Agregada:")
    df_gold.show()
    
    # Salva no MinIO (Delta Lake) e no PostgreSQL (Data Warehouse)
    df_gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    df_gold.write.jdbc(url=POSTGRES_DW_URL, table=GOLD_TABLE_DW, mode="overwrite", properties=POSTGRES_DW_PROPERTIES)

    spark.stop()

if __name__ == "__main__":
    main()