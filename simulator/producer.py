
import argparse
import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

from simulator.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RAW,
    CSV_FILE_PATH,
    TEST_MESSAGES_PATH,
    SAMPLING_INTERVAL_SEC,
    REPLAY_SPEED,
    MAX_ROWS,
    PRODUCER_ACKS,
    PRODUCER_RETRIES,
    PRODUCER_LINGER_MS,
)

from simulator.csv_reader import read_energy_records


def create_producer():
    """Create a connection to Kafka."""

    servers = [
        server.strip()
        for server in KAFKA_BOOTSTRAP_SERVERS.split(",")
    ]

    return KafkaProducer(
        bootstrap_servers=servers,
        acks=PRODUCER_ACKS,
        retries=PRODUCER_RETRIES,
        linger_ms=PRODUCER_LINGER_MS,
    )


def read_test_messages(file_path):
    """Read test messages as raw bytes from a JSON Lines file."""

    with open(file_path, "rb") as file:
        for line in file:
            if line.strip():
                yield line.strip()


def send_messages(producer, messages, max_rows=0, replay_speed=0):
    """Send messages to the raw Kafka topic."""

    sent_count = 0

    delay = (
        SAMPLING_INTERVAL_SEC / replay_speed
        if replay_speed > 0
        else 0
    )

    try:
        for record in messages:

            if max_rows > 0 and sent_count >= max_rows:
                break

            # CSV records are dictionaries.
            # Test messages are already raw bytes.
            if isinstance(record, dict):
                message = json.dumps(
                    record,
                    allow_nan=False
                ).encode("utf-8")

            elif isinstance(record, str):
                message = record.encode("utf-8")

            elif isinstance(record, bytes):
                message = record

            else:
                raise TypeError(
                    f"Unsupported message type: {type(record).__name__}"
                )

            future = producer.send(
                KAFKA_TOPIC_RAW,
                value=message
            )

            # Wait for Kafka to confirm this message.
            metadata = future.get(timeout=30)

            sent_count += 1

            if sent_count == 1 or sent_count % 1000 == 0:
                print(
                    f"Sent {sent_count} messages | "
                    f"Topic: {metadata.topic} | "
                    f"Partition: {metadata.partition}"
                )

            if delay > 0:
                time.sleep(delay)

    except KafkaError:
        print("Kafka error occurred while sending messages.")
        raise

    finally:
        producer.flush()

    print(f"\nFinished! Total messages sent: {sent_count}")


def main():
    parser = argparse.ArgumentParser(
        description="Energy monitoring Kafka producer"
    )

    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Send the separate NiFi test messages instead of CSV data"
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=MAX_ROWS,
        help="Maximum number of CSV records to send; 0 means all"
    )

    parser.add_argument(
        "--replay-speed",
        type=float,
        default=REPLAY_SPEED,
        help="Replay speed multiplier; 0 means maximum speed"
    )

    args = parser.parse_args()

    if args.max_rows < 0 or args.replay_speed < 0:
        parser.error("max-rows and replay-speed cannot be negative")

    project_root = Path(__file__).resolve().parent.parent

    producer = None

    try:
        producer = create_producer()

        print("Connected to Kafka.")
        print(f"Target topic: {KAFKA_TOPIC_RAW}")

        if args.test_mode:

            test_path = project_root / TEST_MESSAGES_PATH

            print(f"Reading test messages from: {test_path}")

            messages = read_test_messages(test_path)

            # Send test cases without replay delays.
            send_messages(
                producer,
                messages,
                max_rows=0,
                replay_speed=0
            )

        else:

            csv_path = project_root / CSV_FILE_PATH

            print(f"Reading dataset from: {csv_path}")
            print(f"Maximum rows: {args.max_rows}")
            print(f"Replay speed: {args.replay_speed}x")

            messages = read_energy_records(
                str(csv_path),
                max_rows=args.max_rows
            )

            send_messages(
                producer,
                messages,
                max_rows=args.max_rows,
                replay_speed=args.replay_speed
            )

    except Exception as error:
        print(f"\nProducer failed: {error}")
        raise

    finally:
        if producer is not None:
            producer.close()


if __name__ == "__main__":
    main()
