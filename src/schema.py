from pyspark.sql import types as T

payload_schema = T.StructType([
    T.StructField("sensorId", T.StringType(), True),
    T.StructField("value", T.DoubleType(), True),
    T.StructField("timestamp", T.LongType(), True),   # epoch millis
])