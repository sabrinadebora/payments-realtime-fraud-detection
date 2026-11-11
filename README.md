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

{
  "before": null,
  "after": {
    "transaction_id": "tx_1001",
    "account_id": "acc_001",
    "card_id": "card_001",
    "amount": 250.75,
    "merchant_id": "merch_010"
  },
  "op": "c",
  "ts_ms": 1738920000000
}

### Join no Flink

INSERT INTO enriched_transactions
SELECT
    t.transaction_id,
    t.account_id,
    a.customer_name,
    a.country,
    t.card_id,
    t.amount,
    t.event_time
FROM transactions t
JOIN accounts FOR SYSTEM_TIME AS OF t.event_time AS a
ON t.account_id = a.account_id;

### Alerta de fraude

Foi implementada a regra **RULE_VELOCITY** no Apache Flink SQL para identificar possíveis fraudes por excesso de transações em um curto período de tempo.

#### Regra

Um alerta é gerado quando o mesmo cartão realiza **3 ou mais transações em até 60 segundos**.

#### Query Flink SQL

```sql
INSERT INTO fraud_alerts
SELECT *
FROM transactions
MATCH_RECOGNIZE (
    PARTITION BY card_id
    ORDER BY event_time
    MEASURES
        FIRST(T.transaction_id) AS first_transaction,
        LAST(T.transaction_id) AS last_transaction,
        COUNT(*) AS total_transactions,
        C*RRENT_TIMESTAMP AS detected_at
   *ONE ROW PER MATCH
    PATTERN*(T{3,}?)
    WITHIN*INTERVAL '60' SECOND
    DEFINE*        T AS TRUE
);
```

*### Eventos que dispar*ram a regra

```*son
[
  {
    "transaction_id": "tx_2001",
    "card_id": "card_001",
    "amount": 150.00,
    "event_time": "2025-08-01T10:00:05Z"
  },
  {
    "transaction_id": "tx_2002",
    "card_id": "card_001",
    "amount": 220.00,
    "event_time": "2025-08-01T10:00:25Z"
  },
  {
    "transaction_id": "tx_2003",
    "card_id": "card_001",
    "amount": 75.00,
    "event_time": "2025-08-01T10:00:50Z"
  }
]
```

#### Alerta *erado

```json
{
  "rule_name": "R*LE_VELOCITY",
  "card_id": "card_0*1",
  "total_transactions": 3,
  "*indow_seconds": 60,
  "risk_level"* "HIGH",
  "detected_at": "2025-08*01T10:00:50Z"
}
```

#### Evidênci*

Arquivo:

```text
docs/fraud-ale*t.png
```

A captura demonstra a e*ecução da regra no Flink SQL e a p*blicação automática do evento no*tópico `fraud_alerts`.

*### Resultado

✅*Transações recebidas via CDC

* Enriquecimento realizado com*sucesso

✅ Regra RULE*VELOCITY acionada

✅ Alerta*publicado no tópico fraud*alerts

✅ Consumidor recebeu*o alerta em tempo real
``*

## Custos

O ambiente foi executado utilizando os créditos educacionais disponibilizados para o laboratório.

### Recursos utilizados

| Serviço | Custo |
|----------|----------|
| Neon PostgreSQL | US$ 0,00 |
| Confluent Cloud (Kafka, Schema Registry e Flink) | Coberto pelos créditos |
| Total consumido durante os testes | Dentro do limite de US$ 100,00 |

### Crédito disponível

```text
US$ 100,00
```

### Evidência

Print do painel Billing do Confluent Cloud mostrando o saldo/crédito disponível e os recursos utilizados.

### Observações

- O ambiente foi criado exclusivamente para fins educacionais.
- Os recursos foram removidos ao final do projeto utilizando `./teardown.sh`.
- Nenhuma cobrança excedeu o crédito disponibilizado para o laboratório.

## Custo Total Declarado

```text
Crédito disponível: US$ 100,00
Consumo realizado: conforme painel Billing do Confluent Cloud.
```
