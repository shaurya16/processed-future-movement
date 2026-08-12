# processing-service

Spring Boot / Kafka Streams service that consumes the `future-transactions` Kafka
topic, dedupes retried/re-published records by their `transactionId` header, and
maintains a running per-(Client_Information, Product_Information) net quantity
aggregate — exposed via:

- `GET /api/v1/report` — JSON
- `GET /api/v1/report/csv` — CSV download (`Output.csv` format)

Design decisions: [docs/superpowers/specs/2026-08-09-processing-service-design.md](../docs/superpowers/specs/2026-08-09-processing-service-design.md).

## Running locally

Run these from the **repo root**:

```bash
docker compose up -d kafka
mvn -q -DskipTests install
mvn -pl processing-service spring-boot:run
```

In a separate terminal, once `ingestion-service` has published some records (see its
own README) and this service has been running long enough to consume them:

```bash
curl http://localhost:8082/api/v1/report
curl http://localhost:8082/api/v1/report/csv
```
