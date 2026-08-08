# processing-service

Spring Boot service that consumes the `future-transactions` Kafka topic, maintains a
running per-(Client_Information, Product_Information) net quantity aggregate (Kafka
Streams), and exposes the current daily summary via REST:

- `GET /api/report` — JSON
- `GET /api/report/csv` — CSV download (`Output.csv` format)
