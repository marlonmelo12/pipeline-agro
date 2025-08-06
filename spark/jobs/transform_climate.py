from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, FloatType, StructType, StructField, DateType

def main():
    # --- Configurações ---
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    BRONZE_BUCKET = "climate-raw"
    SILVER_BUCKET = "climate-silver"

    # --- Inicialização da Spark Session ---
    spark = (
        SparkSession.builder.appName("ClimateStreamingTransformation")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    print("Spark Session para CLIMA iniciada.")

    # --- Leitura do Stream da Camada Bronze ---
    # O schema precisa bater com o JSON salvo pelo consumer
    raw_schema = StructType([
        StructField("date", StringType(), True),
        StructField("hub", StringType(), True),
        StructField("state", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("precipitation_mm", FloatType(), True)
    ])
    
    bronze_path = f"s3a://{BRONZE_BUCKET}/*/*/*/*.json"
    raw_stream_df = spark.readStream.schema(raw_schema).json(bronze_path)

    # --- Transformação ---
    # Seleciona e renomeia as colunas de interesse
    transformed_stream_df = (
        raw_stream_df
        .select(
            col("date").cast(DateType()).alias("data"),
            col("hub").alias("cidade"),
            col("state").alias("estado"),
            col("precipitation_mm").alias("precipitacao_mm")
        )
    )
    print("Transformações de clima definidas.")

    # --- Carga na Camada Silver ---
    silver_path = f"s3a://{SILVER_BUCKET}/daily_precipitation"
    checkpoint_path = f"s3a://{SILVER_BUCKET}/_checkpoints/daily_precipitation"

    query = (
        transformed_stream_df.writeStream
        .format("parquet") # Salvando em CSV como solicitado
        .option("header", "true")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime='2 minutes')
        .start()
    )
    
    print(f"Stream de CLIMA iniciado. Salvando dados em: {silver_path}")
    query.awaitTermination()

if __name__ == "__main__":
    main()