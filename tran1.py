from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Tharun").master("yarn").getOrCreate()
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]

# Create a DataFrame
df = spark.createDataFrame(data, schema=columns)
df.write.mode("overwrite").csv("s3://dataengineerp/output/")

   