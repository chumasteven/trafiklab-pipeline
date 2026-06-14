from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("explore_static").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/static/stop_times.txt")

df.show(5)
df.printSchema()
df.groupBy("stop_id").count().show(15)
