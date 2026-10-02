import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GCS_path="gs://pyspark-practice-1/raw/transactions.csv"
Table_id="project-b5a1bfde-12d9-4a81-8bc.retail_analytics.transactions"
GCS_Temp="gs://sudhir-data-bucket/"

spark=SparkSession.builder.appName("my-pipeline-1").getOrCreate()
df=spark.read.csv(GCS_path,header=True,inferSchema=True)
df1=df.na.fill({"amount": 0})

df2=df1.filter(df1.status=="SUCCESS")
df3 = df2.withColumn("reporting_month", F.date_format(F.col("timestamp"), "yyyyMM")) \
       .withColumn("reporting_week", F.concat_ws("", 
                                                 F.date_format(F.col("timestamp"), "yyyy"), 
                                                 F.weekofyear(F.col("timestamp"))))
(df3.write.format("Bigquery")
    .option('table',Table_id)
    .option("temporaryGCSBucket",GCS_Temp)
    .option("createDisposition", "CREATE_IF_NEEDED") 
    .mode("append")
    .save())
print("Load into Bigquery Completed")