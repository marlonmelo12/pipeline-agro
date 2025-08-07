from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth, sum as spark_sum

def main():
    spark = (
        SparkSession.builder.appName("GoldPrecipitacao")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0,io.delta:delta-spark_2.12:3.1.0")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    SILVER_PATH = "s3a://climate-silver/daily_precipitation"
    GOLD_PATH = "s3a://gold-layer/daily_precipitation"
    POSTGRES_URL = "jdbc:postgresql://postgres-dw:5432/dw_agro"
    POSTGRES_PROPS = {"user": "dw_user", "password": "dw_password", "driver": "org.postgresql.Driver"}

    df = spark.read.parquet(SILVER_PATH)


    df_gold = (
        df
        .withColumn("ano", year(col("data")))
        .withColumn("mes", month(col("data")))
        .withColumn("dia", dayofmonth(col("data")))
        .groupBy("ano", "mes", "dia", "estado")
        .agg(spark_sum("precipitacao_mm").alias("precipitacao_total_mm"))
        .orderBy("ano", "mes", "dia", "estado")
    )

    df_gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
    df_gold.write.jdbc(url=POSTGRES_URL, table="gold_precipitacao_diaria", mode="overwrite", properties=POSTGRES_PROPS)

    spark.stop()

if __name__ == "__main__":
    main()
