PostgreSQL
    │
    │ CDC (Debezium Source Connector)
    ▼
Confluent Cloud
├── payments
├── accounts
├── alerts_fraud
└── Schema Registry
    │
    ▼
Apache Flink SQL
├── Enriquecimento
└── Regra de fraude
    │
    ▼
Topic de saída
├── enriched_payments
└── fraud_alerts
