
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql.types import StringType, FloatType, StructType, StructField

def main():

    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    BRONZE_BUCKET = "commodities-raw"
    SILVER_BUCKET = "commodities-silver"

    spark = (
        SparkSession.builder.appName("CommoditiesStreamingTransformation")
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

    raw_schema = StructType([
        StructField("Data", StringType(), True),
        StructField("Último", StringType(), True),
        StructField("Abertura", StringType(), True),
        StructField("Máxima", StringType(), True),
        StructField("Mínima", StringType(), True),
        StructField("Vol.", StringType(), True),
        StructField("Var%", StringType(), True)
    ])

    bronze_path = f"s3a://{BRONZE_BUCKET}/*/*/*/*.json"

    raw_stream_df = spark.readStream.schema(raw_schema).json(bronze_path)
    print(f"Iniciando stream da camada Bronze: {bronze_path}")

    transformed_stream_df = (
        raw_stream_df
        .select(col("Data").alias("data_str"), col("Último").alias("preco_str"))
        .withColumn("timestamp", to_timestamp(col("data_str"), "dd.MM.yyyy"))
        .withColumn("preco", regexp_replace(col("preco_str"), ",", ".").cast(FloatType()))
        .select("timestamp", "preco")
    )
    print("Transformações definidas: Selecionando e limpando data e último preço.")

    silver_path = f"s3a://{SILVER_BUCKET}/precos_soja"
    checkpoint_path = f"s3a://{SILVER_BUCKET}/_checkpoints/precos_soja"

    query = (
        transformed_stream_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime='2 minutes')
        .start()
    )

    print(f"Stream de transformação iniciado. Salvando dados na camada Silver em: {silver_path}")
    query.awaitTermination()

if __name__ == "__main__":
    main()