import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GCS_path="gs://pyspark-practice-1/raw/transactions.csv"

spark=SparkSession.builder.appName("my-pipeline-1").getOrCreate()
df=spark.read.csv(GCS_path,header=True,inferSchema=True)
df.show()