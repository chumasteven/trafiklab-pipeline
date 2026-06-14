from pyspark.sql import SparkSession
from google.transit import gtfs_realtime_pb2
import pathlib
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("transform_realtime").getOrCreate()

rows = []
for file in pathlib.Path("data/realtime").glob("*.pb"):
    with open(file, "rb") as f:
        entity = gtfs_realtime_pb2.FeedEntity()
        entity.ParseFromString(f.read())

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
