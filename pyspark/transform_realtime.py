from pyspark.sql import SparkSession
from google.transit import gtfs_realtime_pb2
from google.cloud import storage
from pyspark.sql.functions import col
from pyspark.sql.functions import round

spark = SparkSession.builder.appName("transform_realtime").getOrCreate()
storage_client = storage.Client.from_service_account_json("../trafiklab-pipeline-499120-733d37d4cd82.json")


rows = []
for blob in storage_client.list_blobs("trafiklab-raw-data", prefix="gtfs_realtime_v2"):
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(blob.download_as_bytes())

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip_id = entity.trip_update.trip.trip_id

        for stu in entity.trip_update.stop_time_update:
            rows.append(
                (trip_id,
                stu.stop_sequence,
                stu.stop_id,
                stu.arrival.delay,
                stu.arrival.time)
                )

df = spark.createDataFrame(rows, ["trip_id", "stop_sequence", "stop_id", "delay", "arrival_time"])
df.show()
df.printSchema()
print(df.count())
print(df.select("trip_id").distinct().count())


stops_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/static/stops.txt")
stops_df.show(5)
stops_df.printSchema()

# Join the real-time data with the static stops data to get stop names
stops_df = stops_df.withColumn("stop_id", col("stop_id").cast("string")) 
joined_df = df.join(stops_df, on="stop_id", how="left") # here that both dataframes have a column named "stop_id" and we want to join on that column. The "on" parameter specifies the column to join on, and the "how" parameter specifies the type of join (in this case, a left join).
joined_df.select(
    "trip_id",
    "stop_sequence",
    "stop_id",
    "stop_name",
    "delay",
    "arrival_time"
).show()
joined_df.printSchema()
print(joined_df.filter(col("stop_name").isNull()).count())

print(stops_df.filter(col("stop_id").startswith("9022")).count())
stops_df.filter(col("stop_id") == "9022050004565001").show()


# Hop 1
trips_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/static/trips.txt")
trips_df.printSchema()

trips_df = trips_df.withColumn("trip_id", col("trip_id").cast("string"))
joined_df = joined_df.join(trips_df, on="trip_id", how="left")
joined_df.select(
    "trip_id",
    "route_id",
    "stop_sequence",
    "stop_id",
    "stop_name",
    "delay",
    "arrival_time"
).show()

#hop 2
routes_df = spark.read.option("header", "true").option("inferSchema", "true").csv("data/static/routes.txt")
routes_df.printSchema()


joined_df = joined_df.join(routes_df.select(
    "route_id",
    "route_short_name",
    "route_long_name"
), on="route_id", how="left")

final_df = joined_df.select(
    "route_short_name",
    "trip_id",
    "direction_id",
    "stop_sequence",
    "stop_id",
    "stop_name",
    "delay",
    "arrival_time",
)
final_df.show()
final_df.printSchema()

print(joined_df.filter(col("route_short_name").isNull()).count())

final_df = final_df.withColumn("delay_minutes", round(col("delay") / 60, 1))
final_df.show()

from google.cloud import bigquery
pdf = final_df.toPandas()
client = bigquery.Client.from_service_account_json("../trafiklab-pipeline-499120-733d37d4cd82.json")
table_id = "trafiklab-pipeline-499120.trafiklab_staging.realtime_delays"

job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
job = client.load_table_from_dataframe(pdf, table_id, job_config=job_config)
job.result()  # wait for it to finish
print("Loaded", job.output_rows, "rows to", table_id)
