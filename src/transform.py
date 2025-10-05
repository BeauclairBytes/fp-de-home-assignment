from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class StreamTransformer:
    """
    Chainable API:
        transformer.read_spark_stream() \
                   .parse_stream() \
                   .aggregate_stream()

    Then write to Kafka:
        transformer.write_to_kafka().start()
    """

    def __init__(
        self,
        spark: SparkSession,
        payload_schema: StructType,
        *,
        kafka_bootstrap_servers: str,
        input_topic: str,
        output_topic: str,
        checkpoint_dir: str,
    ) -> None:
        self.spark = spark
        self.payload_schema = payload_schema

        # Kafka / sink config
        self.KAFKA_BOOTSTRAP_SERVERS = kafka_bootstrap_servers
        self.INPUT_TOPIC = input_topic
        self.OUTPUT_TOPIC = output_topic
        self.CHECKPOINT_DIR = checkpoint_dir

        # Internal pipeline state
        self._kafka_stream: Optional[DataFrame] = None
        self._parsed_stream: Optional[DataFrame] = None
        self._aggregated_stream: Optional[DataFrame] = None

    # --- Step 1: read ---
    def read_spark_stream(self) -> "StreamTransformer":
        self._kafka_stream = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", self.INPUT_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )
        return self

    # --- Step 2: parse ---
    def parse_stream(self, source_timezone: str = "Europe/Berlin") -> "StreamTransformer":
        if self._kafka_stream is None:
            raise ValueError("read_spark_stream() must be called before parse_stream().")

        kafka_input_stream = self._kafka_stream
        parsed_stream = (
            kafka_input_stream
            .selectExpr("CAST(value AS STRING) as json_str")
            .withColumn("data", F.from_json(F.col("json_str"), self.payload_schema))
            .select(
                F.col("data.sensorId").alias("sensorId"),
                F.col("data.value").alias("value"),
                F.col("data.timestamp").alias("timestamp"),
                F.to_utc_timestamp(
                    F.from_unixtime(F.col("data.timestamp") / 1000),
                    source_timezone
                ).alias("event_ts"),
            )
        )
        self._parsed_stream = parsed_stream
        return self

    # --- Step 3: aggregate ---
    def aggregate_stream(self) -> "StreamTransformer":
        if self._parsed_stream is None:
            raise ValueError("parse_stream() must be called before aggregate_stream().")

        parsed_stream = self._parsed_stream
        aggregated_stream = (
            parsed_stream
            .withWatermark("event_ts", "1 minutes")
            .groupBy(
                F.window(F.col("event_ts"), "1 minute"),  # tumbling window
                F.col("sensorId"),
            )
            .agg(F.avg("value").alias("averageValue"))
            .select(
                F.col("sensorId"),
                F.col("window.start").cast("long").alias("windowStart"),
                F.col("window.end").cast("long").alias("windowEnd"),
                F.col("averageValue"),
            )
        )
        self._aggregated_stream = aggregated_stream
        return self

    # --- Step 4: write ---
    def write_to_kafka(self):
        if self._aggregated_stream is None:
            raise ValueError("aggregate_stream() must be called before write_to_kafka().")

        out_for_kafka = (
            self._aggregated_stream
            .select(
                F.to_json(
                    F.struct(
                        F.col("sensorId"),
                        F.col("windowStart"),
                        F.col("windowEnd"),
                        F.col("averageValue"),
                    )
                ).alias("value")
            )
        )
        return (
            out_for_kafka
            .writeStream
            .format("kafka")
            .outputMode("update")
            .option("kafka.bootstrap.servers", self.KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", self.OUTPUT_TOPIC)
            .option("checkpointLocation", self.CHECKPOINT_DIR)
        )
