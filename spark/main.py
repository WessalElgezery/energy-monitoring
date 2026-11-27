"""
Spark Structured Streaming entry point.
Runs inside the shared Docker network.
"""
import os

from pyspark.sql import SparkSession
from processing import parse_and_transform
from mysql_writer import write_microbatch_to_mysql


KAFKA_BOOTSTRAP = os.getenv("KAFKA_INTERNAL_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_CLEANED", "energy.cleaned")
CHECKPOINT = os.getenv(
    "SPARK_CHECKPOINT",
    "/opt/spark/checkpoints/energy"
)

MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")


def main():
    spark = (
        SparkSession.builder
        .appName("EnergyMonitoringStructuredStreaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        os.getenv("SPARK_LOG_LEVEL", "WARN")
    )

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option(
            "startingOffsets",
            os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
        )
        .option(
            "maxOffsetsPerTrigger",
            os.getenv("SPARK_MAX_OFFSETS", "5000")
        )
        .option("failOnDataLoss", "false")
        .load()
    )

    outputs = parse_and_transform(raw_stream)

    def write_batch(batch_df, batch_id):
        batch_df.persist()
        try:
            write_microbatch_to_mysql(
                batch_df,
                int(batch_id),
                user=MYSQL_USER,
                password=MYSQL_PASSWORD
            )
        finally:
            batch_df.unpersist()

    query = (
        outputs.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(
            processingTime=os.getenv("SPARK_TRIGGER", "10 seconds")
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
