<<<<<<< HEAD
=======
# spark/jobs/transform_commodities.py (VERSÃO FINAL E CORRIGIDA)
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp
from pyspark.sql.types import StringType, FloatType, StructType, StructField

def main():
<<<<<<< HEAD

=======
    # --- Configurações ---
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    BRONZE_BUCKET = "commodities-raw"
    SILVER_BUCKET = "commodities-silver"

<<<<<<< HEAD
=======
    # --- Inicialização da Spark Session ---
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
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

<<<<<<< HEAD
=======
    # --- Leitura do Stream da Camada Bronze ---
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    raw_schema = StructType([
        StructField("data", StringType(), True),
        StructField("último", StringType(), True),
        StructField("abertura", StringType(), True),
        StructField("máxima", StringType(), True),
        StructField("mínima", StringType(), True),
        StructField("vol.", StringType(), True),
        StructField("var%", StringType(), True)
    ])
    
    bronze_path = f"s3a://{BRONZE_BUCKET}/*/*/*/*.json"
    raw_stream_df = (
        spark.readStream
        .option("allowNonNumericNumbers", "true")
        .schema(raw_schema)
        .json(bronze_path)
    )

<<<<<<< HEAD
    raw_stream_df = spark.readStream.schema(raw_schema).json(bronze_path)
    print(f"Iniciando stream da camada Bronze: {bronze_path}")

=======
    # --- Transformação ---
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    transformed_stream_df = (
        raw_stream_df
        # --- CORREÇÃO FINAL AQUI ---
        # Especificamos o formato exato da data que vem do JSON
        .withColumn("timestamp", to_timestamp(col("data"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("preco", regexp_replace(col("último"), ",", ".").cast(FloatType()))
        .select("timestamp", "preco")
    )

<<<<<<< HEAD
=======
    # --- Carga na Camada Silver ---
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    silver_path = f"s3a://{SILVER_BUCKET}/precos_soja"
    checkpoint_path = f"s3a://{SILVER_BUCKET}/_checkpoints/precos_soja"

    query = (
        transformed_stream_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime='2 minutes')
<<<<<<< HEAD
=======
        .partitionBy("timestamp")
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
        .start()
    )
    
    query.awaitTermination()

if __name__ == "__main__":
    main()