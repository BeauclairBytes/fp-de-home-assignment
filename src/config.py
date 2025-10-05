from pydantic import BaseModel, Field


class AppConfig(BaseModel):
    kafka_bootstrap_servers: str = Field(default="broker:29092", description="Kafka bootstrap servers")
    input_topic: str = Field(default="sensor-input", description="Kafka input topic")
    output_topic: str = Field(default="sensor-output", description="Kafka output topic")
    checkpoint_dir: str = Field(default="file:///tmp", description="Directory for Spark checkpoints")
