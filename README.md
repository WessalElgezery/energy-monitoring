# Real-Time Energy Consumption Monitoring

A real-time Big Data streaming pipeline built using the **House 2 electricity dataset (`2022-09.csv`)**. The system simulates live appliance and whole-house power readings, validates and routes events in real time, aggregates hourly metrics, and stores the results in MySQL for SQL analysis.

## 1. End-to-End Pipeline Architecture

* **Valid Flow:** `Python Producer` → `Kafka (energy.raw)` → `Apache NiFi` → `Kafka (energy.cleaned)` → `Spark Structured Streaming` → `MySQL` → `SQL Analysis / Streamlit`

* **Invalid Flow:** `Apache NiFi` → `Kafka (energy.dlq)`

## 2. Technology Stack & Versions

| Component | Technology & Version | 
| ----- | ----- | 
| **Data Prep & Producer** | Python 3.11+, Pandas, `kafka-python` | 
| **Message Broker** | Apache Kafka 3.8.0 (KRaft mode) | 
| **Validation & Routing** | Apache NiFi 1.27.0 | 
| **Stream Processing** | Apache Spark 3.5.3 (Structured Streaming) | 
| **Database** | MySQL 8.0 | 
| **Containerization** | Docker Desktop & Docker Compose | 
| **Analysis & UI** | SQL & Optional Streamlit Dashboard | 

## 3. Repository Structure

```
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
└── .gitignore
```

## 4. Service Endpoints & Addresses

| Service | Host Address | Docker Internal Address | 
| ----- | ----- | ----- | 
| **Apache Kafka** | `localhost:9094` | `kafka:9092` | 
| **MySQL 8.0** | `localhost:3306` | `mysql:3306` | 
| **Apache NiFi UI** | `https://localhost:8443/nifi` | `https://nifi:8443/nifi` | 
| **Apache Spark UI** | `http://localhost:4040` | `http://spark:4040` | 

## 5. Getting Started

### 1. Environment Setup

Clone the repository and configure your local `.env` file with your agreed credentials *(note: `NIFI_PASSWORD` must be at least 12 characters long)*.

### 2. Start Docker Infrastructure

Launch MySQL, Kafka, NiFi, and Spark in detached mode:

```
docker compose up -d
```

Verify that all containers are healthy and running:

```
docker compose ps
```

### 3. Create Kafka Topics

Initialize the three required topics (`energy.raw`, `energy.cleaned`, `energy.dlq`):

```
docker exec -it kafka bash /opt/kafka/custom-scripts/create_topics.sh
```

### 4. Run the Pipeline Components

1. **Host Python Environment:**

   ```
   pip install -r requirements.txt
   ```

2. **Apache NiFi:** Open `https://localhost:8443/nifi`, log in using your `.env` credentials, upload/import `nifi/energy-etl-flow.json`, and start the flow.

3. **Apache Spark Streaming:** Submit the streaming job inside the Spark container:

   ```
   docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/main.py
   ```

4. **Python Producer:** Start streaming readings from the host machine:

   ```
   python simulator/producer.py
   ```

### 5. Stop Services

```
docker compose down
```

*(Use `docker compose down -v` only if you want to wipe persistent volumes and reset the database/NiFi state).*