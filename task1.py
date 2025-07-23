from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, hour

spark = SparkSession.builder.appName("Mini Sample Dataset").getOrCreate()
print("✅ Spark Session Initialized")

data_path = "yellow_tripdata_2021-01.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

avg_fare = df.groupBy("passenger_count").agg(avg("fare_amount").alias("avg_fare"))
peak_hours = df.groupBy("pickup_hour").agg(count("*").alias("trip_count"))
longest_trips = df.orderBy(col("trip_distance").desc()).limit(3)

print("Average Fare by Passenger Count")
avg_fare.show()

print("Peak Pickup Hours")
peak_hours.show()

print("Top 3 Longest Trips")
longest_trips.show(truncate=False)

spark.stop()
print("✅ Spark Session Stopped")
