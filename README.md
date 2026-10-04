# FinGuard Kafka Streaming Project

Real-time financial data streaming platform for fraud detection and transaction monitoring using Apache Kafka and Databricks.

## 📋 Overview

FinGuard is an event-driven architecture for processing high-volume financial transaction streams in real-time. It captures financial events from multiple sources, applies real-time analytics and machine learning models, detects fraudulent patterns, and persists processed data for further analysis.

**Tech:** Apache Kafka, Databricks, PySpark, PostgreSQL, Real-time ML  
**Capabilities:** Streaming analytics, fraud detection, real-time alerts  
**Status:** ⚙️ Advanced Streaming Architecture

---

## 🏗️ Architecture

```
[Financial Data Sources]
    ↓
[Kafka Producers]
    ├→ Transaction Producer
    ├→ Event Producer
    └→ Alert Producer
    ↓
[Apache Kafka Cluster]
    ├→ transactions topic
    ├→ events topic
    ├→ alerts topic
    └→ archive topic
    ↓
[Kafka Consumers]
    ├→ Databricks (Analytics)
    ├→ PostgreSQL (Storage)
    └→ Dashboard
    ↓
[Real-time Insights & Alerts]
```

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| **Streaming** | Apache Kafka |
| **Analytics** | Databricks, PySpark |
| **Storage** | PostgreSQL, Delta Lake |
| **Monitoring** | Prometheus, Grafana |
| **Languages** | Python, Scala, SQL |

---

## 📁 Project Structure

```
finguard_kakfa_streaming_project/
├── kafka/                     # Kafka config
│   ├── config/
│   ├── schemas/               # Avro schemas
│   └── docker-compose.yaml
├── producers/                 # Kafka Producers
├── consumers/                 # Kafka Consumers
├── databricks/                # Spark notebooks
├── sql/                       # Database scripts
├── dashboard/                 # Monitoring UI
├── monitoring/                # Prometheus config
├── tests/                     # Unit tests
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Apache Kafka 3.0+
- PostgreSQL 12+
- Docker & Docker Compose

### Installation

```bash
# Clone repository
git clone https://github.com/vinayshetty777/finguard_kakfa_streaming_project.git
cd finguard_kakfa_streaming_project

# Start services with Docker
docker-compose up -d

# Create Kafka topics
docker exec kafka bash kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1

# Install Python dependencies
pip install -r requirements.txt
```

---

## 📊 Data Flow

```
Raw Transactions
    ↓
[Kafka Producer] → [Kafka Topics]
    ↓
[Parallel Processing]
    ├→ Fraud Detection (ML)
    ├→ Analytics Aggregation
    └→ Alert Generation
    ↓
[Kafka Topics]
    ├→ Alerts (Immediate)
    ├→ Archive (Historical)
    └→ Events (Downstream)
    ↓
[Consumers]
    ├→ Databricks Analytics
    ├→ PostgreSQL Storage
    └→ Dashboard Visualization
```

---

## ✨ Key Features

- **Real-time Processing** - Sub-second latency
- **Fraud Detection** - ML models for anomaly detection
- **Scalable Architecture** - Handles millions of events
- **Data Persistence** - Historical data archival
- **Monitoring** - Real-time metrics and dashboards
- **Stream Processing** - Complex event processing

---

## 🚀 Running Producers & Consumers

### Start Producer

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

transaction = {
    'transaction_id': 'txn_123',
    'amount': 100.00,
    'timestamp': 1234567890
}
producer.send('transactions', transaction)
```

### Start Consumer

```python
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(f"Received: {message.value}")
```

---

## 🤖 Machine Learning Integration

```python
# Spark-based fraud detection
model = PipelineModel.load("dbfs:/models/fraud_detector")

streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .load()

predictions = model.transform(streaming_df)
```

---

## 📈 Monitoring

- **Consumer Lag** - Track processing speed
- **Throughput** - Messages per second
- **Fraud Rate** - Detection accuracy
- **Latency** - End-to-end processing time
- **Error Rates** - System health

---

## 🧪 Testing

```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/integration/ -v

# With coverage
pytest --cov=.
```

---

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| Producer connection failed | Check Kafka broker: `nc -zv localhost 9092` |
| High consumer lag | Scale partitions and consumers |
| Data loss | Verify replication factor ≥ 2 |
| Performance issues | Check downstream bottlenecks |

---

## 🔐 Best Practices

✅ **Do:**
- Enable SASL/SSL authentication
- Use schema registry
- Implement ACLs
- Encrypt data in transit
- Monitor access logs

❌ **Don't:**
- Send credentials in clear text
- Disable authentication
- Use default passwords
- Ignore security warnings

---

## 🤝 Contributing

1. Fork repository
2. Create feature branch
3. Add tests
4. Submit PR

---

## 📚 Resources

- [Apache Kafka Docs](https://kafka.apache.org/documentation/)
- [Confluent Platform](https://docs.confluent.io/)
- [Databricks Streaming](https://docs.databricks.com/en/structured-streaming/)

---

**Last Updated:** 2026-07-13  
**Kafka Version:** 3.0+  
**Python Version:** ≥3.8
