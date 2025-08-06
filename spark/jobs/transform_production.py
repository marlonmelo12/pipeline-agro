# spark/jobs/transform_production.py (VERSÃO FINAL COM FOREACHBATCH)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, window
from pyspark.sql.types import StringType, StructType, StructField, LongType, TimestampType

# --- Configurações ---
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BRONZE_BUCKET = "production-raw"
SILVER_BUCKET = "production-silver"

def process_and_save_batch(df, epoch_id):
    """
    Esta função é chamada para cada micro-lote de dados processados.
    Ela salva o DataFrame do lote na camada Silver usando o modo 'overwrite'.
    """
    silver_path = f"s3a://{SILVER_BUCKET}/daily_total_production"
    
    print(f"--- Processando Lote ID: {epoch_id} ---")
    if not df.isEmpty():
        print(f"Salvando {df.count()} registros atualizados em: {silver_path}")
        # Como estamos salvando o resultado completo do lote, usamos o modo 'overwrite'
        # Isso efetivamente simula o 'outputMode("complete")' para cada lote.
        (df.write
         .mode("overwrite")
         .parquet(silver_path)
        )

def main():
    # --- Inicialização da Spark Session ---
    spark = (
        SparkSession.builder.appName("ProductionStreamingTransformation")
        # ... (a configuração da sessão continua a mesma)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("INFO")
    print("Spark Session para PRODUÇÃO iniciada.")

    # --- Leitura do Stream (continua a mesma) ---
    raw_schema = StructType([
        StructField("data", StringType(), True),
        StructField("unidade_federativa", StringType(), True),
        StructField("produto", StringType(), True),
        StructField("producao", LongType(), True)
    ])
    bronze_path = f"s3a://{BRONZE_BUCKET}/*/*/*/*.json"
    raw_stream_df = spark.readStream.schema(raw_schema).json(bronze_path)

    # --- Transformação com Agregação (continua a mesma) ---
    data_with_timestamp = raw_stream_df.withColumn("timestamp", col("data").cast(TimestampType()))
    daily_total_production = (
        data_with_timestamp
        .withWatermark("timestamp", "10 minutes")
        .groupBy(window(col("timestamp"), "1 day"))
        .agg(_sum("producao").alias("producao_total_toneladas"))
        .select(
            col("window.start").cast("date").alias("data"),
            "producao_total_toneladas"
        )
    )
    
    # --- Carga na Camada Silver usando foreachBatch ---
    checkpoint_path = f"s3a://{SILVER_BUCKET}/_checkpoints/daily_total_production"

    query = (
        daily_total_production.writeStream
        .foreachBatch(process_and_save_batch) # <-- MUDANÇA PRINCIPAL AQUI
        .outputMode("update") # O modo para a agregação continua 'update'
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime='5 minutes')
        .start()
    )
    
    print(f"Stream de PRODUÇÃO iniciado. Usando foreachBatch para salvar na camada Silver.")
    query.awaitTermination()

if __name__ == "__main__":
    main()