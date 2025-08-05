from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date
from pyspark.sql.types import FloatType

# --- Configurações ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

BRONZE_BUCKET = "commodities-raw"
SILVER_BUCKET = "commodities-silver"

def main():
    # --- 1. Inicialização da Spark Session ---
    spark = (
        SparkSession.builder.appName("CommoditiesTransformationStream")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    print("Spark Session iniciada e configurada para o MinIO.")

    # --- 2. Definição do Schema e Leitura do Stream (Bronze) ---
    json_schema = (
        "data STRING, `último` STRING, abertura STRING, máxima STRING, mínima STRING, `vol.` STRING, `var%` STRING"
    )
    
    bronze_path = f"s3a://{BRONZE_BUCKET}/*/*/*/*.json"

    raw_stream_df = spark.readStream.schema(json_schema).json(bronze_path)
    print(f"Iniciando stream da pasta: {bronze_path}")

    # --- 3. Transformações (Somente data e último preço) ---
    transformed_df = (
        raw_stream_df
        .withColumn("data", to_date(col("data"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("ultimo_preco", regexp_replace(col("último"), ",", ".").cast(FloatType()))
        .select("data", "ultimo_preco")
    )
    print("Transformações aplicadas (apenas data e último preço).")

    # --- 4. Escrita do Stream (Silver) ---
    silver_path = f"s3a://{SILVER_BUCKET}/"
    checkpoint_path = f"s3a://{SILVER_BUCKET}/_checkpoints"

    query = (
        transformed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", checkpoint_path)
        .start()
    )

    print(f"Stream iniciado. Salvando dados processados em: {silver_path}")
    print("Aguardando o término do stream (este processo não terminará a menos que seja interrompido)...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
