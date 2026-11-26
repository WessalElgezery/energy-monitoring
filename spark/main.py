"""
Spark Structured Streaming entry point.
Run inside the shared Docker network; Kafka/MySQL addresses are configurable.
"""
import os
from pyspark.sql import SparkSession
from processing import parse_and_transform
from mysql_writer import write_microbatch_to_mysql

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "energy.cleaned")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT", "/checkpoints/energy")
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "energy_monitoring")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    "?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true"
)

def main():
    spark = (
        SparkSession.builder
        .appName("EnergyMonitoringStructuredStreaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka value is JSON text. Processing creates a shared event-level frame
    # and separate frames for the five MySQL destinations.
    outputs = parse_and_transform(raw_stream)

    def write_batch(batch_df, batch_id):
        # Each destination is written independently but with the same Spark batch ID.
        # The writer uses MySQL upserts, making retries safe for the agreed keys.
        write_microbatch_to_mysql(
            batch_df, int(batch_id),
            mysql_url=MYSQL_URL, user=MYSQL_USER, password=MYSQL_PASSWORD
        )

    # A single foreachBatch callback receives parsed event rows and transforms them
    # into the five table payloads before upserting. This avoids multiple stream
    # queries competing for offsets/checkpoints.
    query = (
        outputs.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=os.getenv("SPARK_TRIGGER", "10 seconds"))
        .start()
    )
    query.awaitTermination()

if __name__ == "__main__":
    main()
