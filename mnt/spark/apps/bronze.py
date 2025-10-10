from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CleanBronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.option("header", True).csv("s3a://lakehouse/raw/applicants_clean.csv")

df_clean = df.dropna().dropDuplicates()

# df_clean.write.mode("overwrite").parquet("s3a://lakehouse/bronze/applicants/")
df_clean.write.mode("overwrite").option("compression", "gzip").parquet("s3a://lakehouse/bronze/applicants/")
print(">>> Đã ghi dữ liệu vào lakehouse/bronze/applicants/")
spark.stop()

