# ingestion-service

Spring Boot service that reads the System A fixed-width file (`Input.txt`) using the
parser in [`common`](../common/) and publishes one Kafka event per transaction record
to the `future-transactions` topic.

Stands in for the "Kafka streaming input instead of file" scenario called out in the
requirements — this service is the producer side of that pipeline.

Design decisions: [docs/superpowers/specs/2026-08-09-ingestion-service-design.md](../docs/superpowers/specs/2026-08-09-ingestion-service-design.md).

## Running locally

```bash
docker compose up -d          # starts a local Kafka broker on localhost:9092
mvn -pl ingestion-service -am spring-boot:run
```

By default it reads `sample-data/Input.txt` (relative to the repo root). Override with
`INGESTION_FILE_PATH=/path/to/file`.

## API

- `POST /api/ingest` — parses the configured file and publishes each record to Kafka.
  Returns a JSON body: `{fingerprint, totalLines, published, skipped, errors, cached}`.
  Calling it again for the same (unchanged) file returns the cached result without
  republishing (`cached: true`).
- `POST /api/ingest?force=true` — bypasses the cache and republishes even if this exact
  file was already ingested.

```bash
curl -X POST http://localhost:8081/api/ingest
```
