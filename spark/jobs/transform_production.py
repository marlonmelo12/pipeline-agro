from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, ArrayType, DateType

def main():
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    BRONZE_BUCKET = "production-raw"
    SILVER_BUCKET = "production-silver"

    spark = (
        SparkSession.builder.appName("ProductionSimpleTransformation")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    print("Spark Session para PRODUÇÃO (simples) iniciada.")

    bronze_path = f"s3a://{BRONZE_BUCKET}/*/*/*/*.json"
    
    raw_text_df = spark.readStream.format("text").load(bronze_path)

    single_record_schema = StructType([
        StructField("data", StringType(), True),
        StructField("unidade_federativa", StringType(), True),
        StructField("produto", StringType(), True),
        StructField("producao", DoubleType(), True)
    ])
<<<<<<< HEAD
  
=======
    
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    parsed_df = raw_text_df.withColumn(
        "json_data", from_json(col("value"), ArrayType(single_record_schema))
    )
    
<<<<<<< HEAD

=======
>>>>>>> c12b6cae63b4907569f36ba8b7466d38a72573b2
    exploded_df = parsed_df.select(explode(col("json_data")).alias("data"))
    
    structured_df = exploded_df.select("data.*")

    transformed_stream_df = (
        structured_df
        .withColumn("data", col("data").cast(DateType()))
        .select(
            col("data"),
            col("unidade_federativa").alias("estado"),
            col("producao")
        )
    )
    print("Transformações simples de PRODUÇÃO definidas.")

    silver_path = f"s3a://{SILVER_BUCKET}/production_data"
    checkpoint_path = f"s3a://{SILVER_BUCKET}/_checkpoints/production_data"

    query = (
        transformed_stream_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", silver_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime='2 minutes')
        .partitionBy("data") 
        .start()
    )
    
    print(f"Stream de PRODUÇÃO (simples) iniciado. Salvando dados em: {silver_path}")
    query.awaitTermination()

if __name__ == "__main__":
    main()