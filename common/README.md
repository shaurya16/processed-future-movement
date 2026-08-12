# common

Shared library used by both `ingestion-service` and `processing-service`, so the
fixed-width parsing logic and domain model exist in exactly one place.

Planned contents:
- `FutureTransactionRecord` — parsed representation of one 176-byte input record
- `FixedWidthRecordParser` — position-based parser driven by the field table in
  [`docs/file-spec.md`](../docs/file-spec.md)
- Kafka message schema (the JSON/Avro contract published by `ingestion-service` and
  consumed by `processing-service`)
- `ReportKey` (client info + product info) and aggregation value types

`common` also ships `src/main/resources/pfm-defaults.yml`, a shared Spring Boot
config-data resource holding the Kafka topic-name default, imported by both
`ingestion-service` and `processing-service` via `spring.config.import`. `common`
itself has no Spring dependency, but it's the one classpath both services already
share, which is why the file lives here rather than in a new module.
