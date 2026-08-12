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

## Upgrading past the ReportKey change

The Kafka message key changed from two concatenated fields to all eight
(`ReportKey`), and the `net-quantity-store` value changed from a `Long` to a
`NetPosition`. Neither the store name nor the `application-id` was renamed —
a version baked into an identifier outlives the migration that caused it — so
existing state must be discarded rather than migrated.

No deployment path in this repo persists state, so in practice there is nothing
to clean up: `docker-compose.yml` declares no volumes at all, and
`k8s/kafka.yaml` uses `emptyDir: {}` with no `volumeMounts` on
processing-service. Both are pod/container-lifetime only.

The one exception is the broker-only development loop (`docker compose up -d
kafka` with the services run via Maven), where Kafka Streams keeps RocksDB state
on the **host**:

```bash
docker compose down -v                                    # containerised paths
rm -rf "${TMPDIR:-/tmp}/kafka-streams/processing-service"  # broker-only loop, host-side state
```

`${TMPDIR:-/tmp}` matters: Kafka Streams derives `state.dir` from `java.io.tmpdir`,
which is `/tmp` on Linux but a per-user `/var/folders/.../T/` on macOS. A
hardcoded `/tmp/kafka-streams` silently no-ops on macOS — the exact machine where
the broker-only loop runs.

Kafka Streams defaults `auto.offset.reset` to `earliest` (unlike a plain
consumer, which defaults to `latest`), so after teardown the topic replays from
the beginning and the store rebuilds with the new key format automatically.
