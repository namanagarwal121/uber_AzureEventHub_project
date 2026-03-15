from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

EH_NAMESPACE = "ehuberevents"
EH_NAME      = "ubertopic"

EH_CONN_STR  = spark.conf.get("connection_string")

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
  "kafka.request.timeout.ms" : 100000,
  "kafka.session.timeout.ms" : 100000,
  "maxOffsetsPerTrigger"     : 100000,
  "failOnDataLoss"           : 'true',
  "startingOffsets"          : 'earliest'
}

CHECKPOINT_PATH = "/Volumes/uber/bronze/my_volume/volume_folder/"
dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)

@dp.table
def rides_raw():
  df = spark.readStream.format("kafka") \
      .options(**KAFKA_OPTIONS) \
      .load()
  #converting values to string
  # df_transformed = df.withColumn("rides", col("value").cast("string"))
  return df


