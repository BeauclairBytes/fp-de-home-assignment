

from spark_utils import get_spark_session
from transform import StreamTransformer
from schema import payload_schema
from config import AppConfig


KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
INPUT_TOPIC = "sensor-input"
OUTPUT_TOPIC = "sensor-output"
CHECKPOINT_DIR  = "file:///tmp"


def main():
    spark = get_spark_session()
    config = AppConfig()
    transformer = StreamTransformer(
        spark=spark,
        payload_schema=payload_schema,
        kafka_bootstrap_servers=config.kafka_bootstrap_servers,
        input_topic=config.input_topic,
        output_topic=config.output_topic,
        checkpoint_dir=config.checkpoint_dir,
    )
    query = (
        transformer
        .read_spark_stream()
        .parse_stream() 
        .aggregate_stream()
        .write_to_kafka()
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()