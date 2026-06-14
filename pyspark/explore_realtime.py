from google.transit import gtfs_realtime_pb2

with open("data/realtime/gtfs_realtime_2026-06-14T06-40-30.pb", "rb") as f:
    entity = gtfs_realtime_pb2.FeedEntity()
    entity.ParseFromString(f.read())
print(entity)