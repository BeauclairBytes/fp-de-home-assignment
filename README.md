# FP DE Home Assignment

This solution uses Spark Structured Streaming to read the data from Kafka, process it, and save it again in the output topic. 

The transformation uses watermark and window to keep data from 1 minute and later on drop informaiton from the state information. This data is aggregated and then written to Kafka in the update mode. The update mode is used to guarantee that changes for a given sensor and window is sent to kafka.



# Execution

The Spark application can be executed with docker in the same Docker Compose file as Kafka. After initializing Kafka and populating it with data, you can start Spark with the following command: 

```bash
docker-compose up --build spark-etl
```


Alternatively, if you have Spark locally installed, it is possible to run the job with `spark-submit`:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 --jars spark/jars/spark-streaming-kafka-0-10_2.13-4.0.1.jar  src/main.py
```
# fp-de-home-assignment
# fp-de-home-assignment
