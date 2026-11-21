# Real-Time Energy Consumption Monitoring

A real-time Big Data streaming pipeline built using the **House 2 electricity dataset (`2022-09.csv`)**. The system simulates live appliance and whole-house power readings, validates and routes events in real time, aggregates hourly metrics, and stores the results in MySQL for SQL analysis[cite: 2].

---

## 1. End-to-End Pipeline Architecture

* **Valid Flow:** `Python Producer` → `Kafka (energy.raw)` → `Apache NiFi` → `Kafka (energy.cleaned)` → `Spark Structured Streaming` → `MySQL` → `SQL Analysis / Streamlit`[cite: 2]
* **Invalid Flow:** `Apache NiFi` → `Kafka (energy.dlq)`[cite: 2]

---

## 2. Technology Stack & Versions

| Component | Technology & Version |
| :--- | :--- |
| **Data Prep & Producer** | Python 3.11+, Pandas, `kafka-python`[cite: 3, 6] |
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
│   └── 2022-09.csv              # Original House 2 dataset (120,960 rows, 10s interval)[cite: 2, 4]
├── tests/
│   └── test_messages.jsonl      # 10 NiFi validation test cases (5 valid, 5 invalid)[cite: 4, 9]
├── simulator/
│   ├── config.py                # Kafka addresses, topics, and replay speed settings[cite: 4, 6]
│   ├── csv_reader.py            # CSV parser preserving nulls and timestamps[cite: 4, 6]
│   └── producer.py              # Kafka JSON producer (--test-mode & replay speed)[cite: 4, 6]
├── kafka/
│   └── create_topics.sh         # Creates energy.raw, energy.cleaned, and energy.dlq[cite: 4, 6]
├── nifi/
│   └── energy-etl-flow.json     # Exported NiFi validation and routing flow[cite: 4, 6]
├── spark/
│   ├── main.py                  # Spark Structured Streaming entry point[cite: 4, 7]
│   ├── processing.py            # Schema, transformations, and hourly aggregations[cite: 4, 7]
│   └── mysql_writer.py          # Retry-safe JDBC writer to MySQL[cite: 4, 8]
├── database/
│   └── init.sql                 # DDL for the 5 MySQL tables[cite: 4, 8]
├── sql/
│   └── queries.sql              # Analytical SQL queries[cite: 4, 8]
├── dashboard/                   # Optional Streamlit visualization[cite: 4, 8]
│   └── app.py
├── docker-compose.yml           # Full infrastructure setup[cite: 4, 9]
├── requirements.txt             # Host-side Python dependencies[cite: 4, 5]
├── .env.example                 # Environment variable template[cite: 4, 5]
└── .gitignore
