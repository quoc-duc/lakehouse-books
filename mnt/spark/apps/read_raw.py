from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Step1_ReadRaw") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

df = spark.read.option("header", True).csv("s3a://lakehouse/raw/applicants_clean.csv")

print(">>> Tổng số dòng:", df.count())
df.show(5)

spark.stop()
