from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.sql("select * from uber.bronze.bulk_rides")
rides_schema = df.schema

dp.create_streaming_table("stg_rides")

# Initial bulk load
@dp.append_flow(
    target="stg_rides",
    once=True
)
def rides_bulk():

    df = spark.read.table("uber.bronze.bulk_rides")

    df = df.withColumn(
        "booking_timestamp",
        col("booking_timestamp").cast("timestamp")
    )

    return df


# Streaming ingestion
@dp.append_flow(
    target="stg_rides"
)
def rides_stream():

    df = spark.readStream.table("uber.bronze.rides_raw")

    df = df.withColumn("rides", col("value").cast("string"))

    df_parsed = df.withColumn(
        "parsed_rides",
        from_json("rides", rides_schema)
    ).select("parsed_rides.*")

    df_parsed = df_parsed.withColumn(
        "booking_timestamp",
        col("booking_timestamp").cast("timestamp")
    )

    return df_parsed