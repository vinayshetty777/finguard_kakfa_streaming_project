# FinGuard Kafka Streaming Project

A real-time financial data streaming platform built with Apache Kafka, Databricks, and PostgreSQL. Processes financial transactions, detects anomalies, and delivers insights for fraud prevention and monitoring.

## 🎯 Project Overview

FinGuard is an event-driven architecture for processing high-volume financial transaction streams in real-time. It captures financial events from multiple sources, applies real-time analytics and machine learning models, detects fraudulent patterns, and persists processed data for further analysis.

**Key Objectives:**
- Real-time transaction processing
- Fraud detection and prevention
- Event-driven architecture implementation
- Scalable data streaming
- Historical data archival and analysis

## 🏗️ Architecture

```
[Financial Data Sources]
(Payment Systems, APIs, Feeds)
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
├→ Databricks (Real-time Analytics)
├→ PostgreSQL (Historical Storage)
└→ Monitoring Dashboard
    ↓
[Real-time Insights & Alerts]
```

## 🛠️ Tech Stack

- **Streaming Platform:** Apache Kafka
- **Cluster Deployment:** Confluent Cloud or Self-Hosted
- **Analytics & Processing:** Databricks, PySpark
- **Data Storage:** PostgreSQL, Delta Lake
- **Monitoring:** Kafka UI, Prometheus, Grafana
- **Languages:** Python, Scala, SQL
- **Visualization:** Custom Dashboard, Power BI

## 📁 Project Structure

```
finguard_kakfa_streaming_project/
├── kafka/                      # Kafka configuration & setup
│   ├── config/
│   │   ├── server.properties
│   │   ├── topics.yaml
│   │   └── producer.properties
│   ├── schemas/                # Avro schemas
│   │   ├── transaction_schema.avsc
│   │   ├── event_schema.avsc
│   │   └── alert_schema.avsc
│   └── docker-compose.yaml
├── producers/                  # Kafka Producers
│   ├── transaction_producer.py
│   ├── event_producer.py
│   └── utils.py
├── consumers/                  # Kafka Consumers
│   ├── databricks_consumer.py
│   ├── postgres_consumer.py
│   └── alert_consumer.py
├── databricks/                 # Databricks Notebooks
│   ├── streaming_pipeline.py
│   ├── fraud_detection.py
│   ├── analytics.py
│   └── model_training.py
├── sql/                        # PostgreSQL scripts
│   ├── schema_setup.sql
│   ├── indexes.sql
│   └── procedures.sql
├── dashboard/                  # Monitoring & visualization
│   ├── app.py (Streamlit/Flask)
│   ├── templates/
│   └── static/
├── monitoring/                 # Observability
│   ├── prometheus_config.yaml
│   ├── grafana_dashboards/
│   └── alerts.yaml
├── tests/                      # Unit & integration tests
├── pyproject.toml
└── README.md
```

## 🚀 Setup & Installation

### Prerequisites
- Python 3.8+
- Apache Kafka 3.0+
- PostgreSQL 12+
- Databricks workspace
- Docker & Docker Compose (for local setup)

### Quick Start with Docker

```bash
# Clone repository
git clone https://github.com/vinayshetty777/finguard_kakfa_streaming_project.git
cd finguard_kakfa_streaming_project

# Start Kafka cluster and dependencies
docker-compose up -d

# Create topics
docker exec kafka bash kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1
```

### Manual Setup

1. **Install Kafka:**
   ```bash
   wget https://archive.apache.org/dist/kafka/3.0.0/kafka_2.13-3.0.0.tgz
   tar -xzf kafka_2.13-3.0.0.tgz
   cd kafka_2.13-3.0.0
   ```

2. **Start Zookeeper and Kafka:**
   ```bash
   # Terminal 1
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Terminal 2
   bin/kafka-server-start.sh config/server.properties
   ```

3. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure PostgreSQL:**
   ```bash
   psql -U postgres -f sql/schema_setup.sql
   ```

## 📊 Data Flow

### Transaction Processing Pipeline

```
1. Raw Transactions
   ↓
2. [Kafka Producer] → captures transactions
   ↓
3. [Kafka Topic: transactions]
   ↓
4. [Parallel Processing]
   ├→ Fraud Detection (Real-time ML)
   ├→ Analytics Aggregation
   └→ Alert Generation
   ↓
5. [Kafka Topics]
   ├→ Alerts (for immediate action)
   ├→ Archive (for historical storage)
   └→ Events (for downstream systems)
   ↓
6. [Consumers]
   ├→ Databricks (Analytics)
   ├→ PostgreSQL (Archival)
   └→ Dashboard (Visualization)
```

## 🔄 Kafka Topics & Schemas

### Topics Configuration

**transactions** (Primary data stream)
```json
{
  "partitions": 3,
  "replication_factor": 2,
  "retention_ms": 604800000,  // 7 days
  "schema": "transaction_schema.avsc"
}
```

**alerts** (Real-time fraud alerts)
```json
{
  "partitions": 1,
  "replication_factor": 2,
  "retention_ms": 86400000,   // 24 hours
  "schema": "alert_schema.avsc"
}
```

### Example Transaction Schema

```json
{
  "namespace": "com.finguard.transaction",
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "amount", "type": "double"},
    {"name": "merchant_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "merchant_category", "type": "string"},
    {"name": "country", "type": "string"}
  ]
}
```

## 🚀 Running Producers & Consumers

### Start Transaction Producer

```python
# producers/transaction_producer.py
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send transactions
for i in range(1000):
    transaction = {
        'transaction_id': f'txn_{i}',
        'amount': 100.00,
        'timestamp': int(time.time() * 1000)
    }
    producer.send('transactions', transaction)
```

### Start Consumer

```python
# consumers/postgres_consumer.py
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    # Process and store in PostgreSQL
    store_transaction(message.value)
```

## 🤖 Machine Learning Integration

### Fraud Detection Model

```python
# databricks/fraud_detection.py
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

# Load pre-trained model
model = PipelineModel.load("dbfs:/models/fraud_detector")

# Apply to streaming data
streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .load()

predictions = model.transform(streaming_df)

# Write results
predictions \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "alerts") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

## 📊 Monitoring & Observability

### Prometheus Metrics

```yaml
# monitoring/prometheus_config.yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'kafka-broker'
    static_configs:
      - targets: ['localhost:9308']
  
  - job_name: 'consumer-lag'
    static_configs:
      - targets: ['localhost:9308']
```

### Grafana Dashboard

Key metrics to monitor:
- Consumer lag
- Transaction throughput
- Fraud detection rate
- Processing latency
- Error rates

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
pytest tests/

# Run specific test
pytest tests/test_producers.py -v

# Coverage report
pytest --cov=. tests/
```

### Integration Tests

```bash
# Start services
docker-compose up -d

# Run integration tests
pytest tests/integration/

# Stop services
docker-compose down
```

## 🐛 Troubleshooting

### Issue: Producer Connection Failed
- Check Kafka broker is running: `nc -zv localhost 9092`
- Verify bootstrap servers configuration
- Check network connectivity

### Issue: High Consumer Lag
- Scale up partition count
- Increase consumer instances
- Optimize processing logic
- Check downstream bottlenecks

### Issue: Data Loss in Kafka
- Verify replication factor ≥ 2
- Check retention policies
- Enable topic persistence
- Review broker logs

## 🔐 Security Best Practices

✅ **Do:**
- Enable SASL/SSL authentication
- Use schema registry
- Implement ACLs
- Encrypt data in transit & at rest
- Monitor access logs

❌ **Don't:**
- Send credentials in clear text
- Disable authentication for convenience
- Use default passwords
- Ignore security warnings

## 📚 Documentation

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Platform Guide](https://docs.confluent.io/)
- [Databricks Structured Streaming](https://docs.databricks.com/en/structured-streaming/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create feature branch
3. Add tests for changes
4. Submit pull request

## 📄 License

This project is open source under MIT License.

---

**Project Status:** Active Development  
**Last Updated:** 2026-07-13  
**Kafka Version:** 3.0+  
**Python Version:** ≥3.8
