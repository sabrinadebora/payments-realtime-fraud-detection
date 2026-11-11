# Pipeline de Pagamentos em Tempo Real com Confluent Cloud

Projeto desenvolvido para o desafio da DIO.

## Tecnologias

- PostgreSQL
- Debezium CDC
- Confluent Cloud
- Kafka
- Schema Registry
- Apache Flink SQL

## Arquitetura

PostgreSQL
→ CDC (Debezium)
→ Confluent Cloud
→ Flink SQL
→ Fraud Alerts

## Regra de Fraude

3 transações do mesmo cartão em 60 segundos geram um alerta.

## Evidências

### CDC funcionando

[imagem]

### Join no Flink

[imagem]

### Alerta de fraude

[imagem]

## Custos

| Serviço | Valor |
|----------|---------|
| Kafka | US$ 100 |
| Flink | US$ 100 |
| Total | US$ 100 |
