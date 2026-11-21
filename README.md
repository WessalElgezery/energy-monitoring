# Real-Time Energy Consumption Monitoring

A real-time Big Data streaming pipeline built using the **House 2 electricity dataset (`2022-09.csv`)**. The system simulates live appliance and whole-house power readings, validates and routes events in real time, aggregates hourly metrics, and stores the results in MySQL for SQL analysis.

---

## 1. End-to-End Pipeline Architecture

* **Valid Flow:** `Python Producer` → `Kafka (energy.raw)` → `Apache NiFi` → `Kafka (energy.cleaned)` → `Spark Structured Streaming` → `MySQL` → `SQL Analysis / Streamlit`
* **Invalid Flow:** `Apache NiFi` → `Kafka (energy.dlq)`

---

## 2. Technology Stack & Versions

| Component | Technology & Version |
| :--- | :--- |
| **Data Prep & Producer** | Python 3.11+, Pandas, `kafka-python` |
| **Message Broker** | Apache Kafka 3.8.0 (KRaft mode) |
| **Validation & Routing** | Apache NiFi 1.27.0 |
| **Stream Processing** | Apache Spark 3.5.3 (Structured Streaming) |
| **Database** | MySQL 8.0 |
| **Containerization** | Docker Desktop & Docker Compose |
| **Analysis & UI** | SQL & Optional Streamlit Dashboard |

---

## 3. Repository Structure

```text
energy-monitoring/
├── data/
│   └── 2022-09.csv              # Original House 2 dataset (120,960 rows, 10s interval)
├── tests/
│   └── test_messages.jsonl      # 10 NiFi validation test cases (5 valid, 5 invalid)
├── simulator/
│   ├── config.py                # Kafka addresses, topics, and replay speed settings
│   ├── csv_reader.py            # CSV parser preserving nulls and timestamps
│   └── producer.py              # Kafka JSON producer (--test-mode & replay speed)
├── kafka/
│   └── create_topics.sh         # Creates energy.raw, energy.cleaned, and energy.dlq
├── nifi/
│   └── energy-etl-flow.json     # Exported NiFi validation and routing flow
├── spark/
│   ├── main.py                  # Spark Structured Streaming entry point
│   ├── processing.py            # Schema, transformations, and hourly aggregations
│   └── mysql_writer.py          # Retry-safe JDBC writer to MySQL
├── database/
│   └── init.sql                 # DDL for the 5 MySQL tables
├── sql/
│   └── queries.sql              # Analytical SQL queries
├── dashboard/                   # Optional Streamlit visualization
│   └── app.py
├── docker-compose.yml           # Full infrastructure setup
├── requirements.txt             # Host-side Python dependencies
├── .env.example                 # Environment variable template
└── .gitignore
