import os
from dotenv import load_dotenv

load_dotenv()

#Kafka connection
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")

#Topic names & input files
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "energy.raw")
KAFKA_TOPIC_CLEANED = os.getenv("KAFKA_TOPIC_CLEANED", "energy.cleaned")
KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "energy.dlq")

CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "data/2022-09.csv")
TEST_MESSAGES_PATH = os.getenv("TEST_MESSAGES_PATH", "tests/test_messages.jsonl")

#Replay settings
SAMPLING_INTERVAL_SEC = 10.0                      # original dataset interval
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "1000.0"))  # 1000x -> ~20 min for all rows; 0 = max speed
MAX_ROWS = int(os.getenv("MAX_ROWS", "0"))        # 0 = send all rows

#Producer settings
_acks = os.getenv("PRODUCER_ACKS", "all")
PRODUCER_ACKS = _acks if _acks == "all" else int(_acks)
PRODUCER_RETRIES = int(os.getenv("PRODUCER_RETRIES", "5"))
PRODUCER_LINGER_MS = int(os.getenv("PRODUCER_LINGER_MS", "10"))