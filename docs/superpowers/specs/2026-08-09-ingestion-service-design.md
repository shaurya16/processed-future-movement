# Design: `ingestion-service` — REST-triggered Kafka producer

Status: Approved
Branch: `feature/ingestion-service`

## Context

Second buildable slice of processed-future-movement, following `common` (see
[2026-08-09-common-fixed-width-parser-design.md](2026-08-09-common-fixed-width-parser-design.md)).
Per the chosen architecture (see [README.md](../../../README.md)):

```
Input.txt --> ingestion-service --> Kafka topic --> processing-service --> REST API --> frontend
                                                          (Kafka Streams
                                                           aggregation)
```

`ingestion-service` is the producer side: it reads `Input.txt` using `common`'s
`FutureTransactionParser`, and publishes one Kafka event per parsed record to the
`future-transactions` topic. It has no aggregation or reporting responsibility — that's
`processing-service`, a later slice.

## Decisions made during brainstorming

1. **Trigger mechanism**: `POST /api/ingest` REST endpoint, not a `CommandLineRunner`
   and not a directory watcher. `Input.txt` is a static local sample, not a live feed —
   an explicit trigger is easy to demo (`curl`), easy to test, and matches the "producer
   service" mental model without the extra infra a directory watcher would need for one
   sample file.
   - **Why not startup/`CommandLineRunner`**: blurs "service" with "one-shot job" and
     can't be re-run without a restart.
   - **Why not directory watch**: closer to a real production ingestion pattern, but
     meaningfully more infra/complexity than this scope calls for (YAGNI).
2. **File path**: externally configurable via a `ingestion.file.path` property,
   overridable with env var `INGESTION_FILE_PATH` (Spring relaxed binding) — never
   hardcoded. Required for the file to be swappable in different environments/k8s
   without a rebuild.
3. **Duplicate-POST / idempotency handling**: fingerprint the file as
   `path + size + last-modified-time`. An in-memory `ConcurrentHashMap<fingerprint,
   IngestionResult>` guards ingestion via `computeIfAbsent`, so:
   - A repeat call for an unchanged file returns the cached result (`cached: true`)
     without republishing to Kafka.
   - Two concurrent calls for the same file don't race — the second blocks on the
     map's per-key computation and receives the first call's result.
   - `POST /api/ingest?force=true` bypasses the cache and re-ingests/republishes
     regardless, as an explicit escape hatch for demos/re-runs.
   - **Why not always republish (push dedup downstream)**: would require
     `processing-service` to do idempotent per-record upsert instead of a simple sum,
     pushing real complexity into a service that hasn't been designed yet.
   - **Why not reject-concurrent-only (in-flight lock)**: doesn't solve the more common
     case of two sequential calls for the same file minutes apart.
   - **Documented limitation**: the cache is in-process only, so this is only correct
     with a single `ingestion-service` replica. It's a one-shot producer utility, not
     something that needs horizontal scale, so the k8s manifest for this service should
     pin `replicas: 1` rather than building a shared/distributed dedup store.
4. **Serialization format**: JSON via Spring Kafka's `JsonSerializer`, not Avro +
   Schema Registry. No extra infra to deploy/configure/document in k8s, human-readable
   in the topic for demo purposes, and Jackson handles `LocalDate`/`BigDecimal` cleanly.
   Traded off: no enforced schema-compatibility checking — acceptable at this volume/
   scope (717 sample records, one producer, one consumer).
5. **Message payload**: the full `FutureTransaction` domain record from `common` (every
   field), not a slim projection of just the fields today's daily-summary aggregation
   needs. Keeps `common` as the single schema source and avoids a second projection
   schema to keep in sync; a future report needing another field doesn't require a
   producer change.
6. **Message key**: composite `Client_Information|Product_Information` — the exact
   grouping fields the daily summary aggregates by. Guarantees every event for a given
   (client, product) group lands on the same Kafka partition in order, which is what
   makes stateful per-key aggregation in `processing-service`'s Kafka Streams topology
   correct and efficient with no repartition step. Rejected no-key (round-robin)
   because it would push an explicit `groupBy` repartition stage onto the consumer.
7. **Producer reliability**: `acks=all`, `enable.idempotence=true`. Cheap to configure,
   avoids duplicate/lost sends across retries.
8. **Topic management**: `future-transactions` declared via a Spring `NewTopic` bean
   (3 partitions, replication factor 1 for local/dev), not left to broker auto-create.
9. **Error handling**: `common`'s existing skip-and-collect `parseAll()` result is
   surfaced directly, not hidden or turned into a hard failure. Successfully parsed
   records are published; parse errors are logged at `WARN` and returned in the response
   body's `errors` array; one bad line never blocks the rest of the file. Kafka send
   failures are collected the same way; if every send fails (e.g. broker unreachable),
   the endpoint returns 5xx since nothing actually reached the topic.
   - **Why not fail the whole request on any bad line**: contradicts the resilient
     skip-and-collect design `common` was already built for.
10. **Testing infra**: Testcontainers-backed Kafka for integration tests (real broker,
    exercises the actual producer/serializer config), not `spring-kafka-test`'s embedded
    broker. Slower test startup, but more faithful to production behavior, and reuses
    the same Docker dependency the local-dev `docker-compose.yml` needs anyway.

## Components

- **`IngestionProperties`** — `@ConfigurationProperties("ingestion")` binding
  `file.path` (and the Kafka topic name, if not left to `spring.kafka` defaults).
- **`IngestionController`** — exposes `POST /api/ingest` (with optional `force`
  query param), delegates to `IngestionService`, returns `IngestionResult` as the
  response body with an appropriate HTTP status.
- **`IngestionService`** — orchestrates a single ingestion run:
  1. Resolve the configured file path, compute its fingerprint.
  2. `registry.computeIfAbsent(fingerprint, fp -> runIngestion(path))` (or bypass via
     direct `put` when `force=true`).
  3. `runIngestion`: read all lines, call `FutureTransactionParser.parseAll(lines)`,
     publish each successfully parsed `FutureTransaction` via `KafkaTemplate` keyed by
     `Client_Information|Product_Information`, collect per-send outcomes, and assemble
     the final `IngestionResult`.
- **`IngestionRegistry`** — thin wrapper around the `ConcurrentHashMap<String,
  IngestionResult>` fingerprint cache; isolated so its concurrency behavior is
  independently unit-testable.
- **`IngestionResult`** — response/record shape: `fingerprint`, `totalLines`,
  `published`, `skipped`, `errors: List<ParseError>` (reusing `common`'s `ParseError`),
  `cached`.
- **Kafka config** — `NewTopic` bean for `future-transactions`; producer factory config
  (`JsonSerializer`, `acks=all`, `enable.idempotence=true`).

## Data flow

1. Client calls `POST /api/ingest` (optionally `?force=true`).
2. `IngestionController` delegates to `IngestionService`.
3. `IngestionService` resolves the configured file path and fingerprints it
   (path + size + last-modified-time).
4. If already ingested and not forced: return the cached `IngestionResult`
   (`cached: true`), no Kafka activity.
5. Otherwise: read the file's lines, run `FutureTransactionParser.parseAll(lines)` →
   `ParseResult(records, errors)`.
6. For each parsed `FutureTransaction`, publish to `future-transactions` keyed by
   `Client_Information|Product_Information`, value = full record as JSON.
7. Await all sends; assemble `IngestionResult` (published/skipped/errors/cached=false),
   store it in the registry under the fingerprint, return it.

## Testing

- `IngestionRegistry` unit tests: `computeIfAbsent` semantics (repeat call returns
  cached result, no re-execution), `force` bypass overwrites the cached entry.
- Fingerprinting unit tests: same file → same fingerprint; changed size/mtime → new
  fingerprint.
- `IngestionController` unit tests (mocked `IngestionService`): request/response
  shape, `force` param wiring, status codes.
- Integration test (Testcontainers Kafka + real file read over
  `sample-data/Input.txt`): `POST /api/ingest` publishes exactly 717 messages to
  `future-transactions`, each keyed correctly, values deserialize back to the expected
  `FutureTransaction`; a second `POST /api/ingest` (no `force`) publishes zero
  additional messages and returns `cached: true`; `force=true` republishes.
- Error-path integration test: a corrupted/truncated copy of a few lines from the
  sample file still publishes the good records and reports the bad ones in `errors`.

## Out of scope for this slice

- `processing-service` (Kafka Streams consumer/aggregation, REST API) — next slice.
- `docker-compose.yml` for local Kafka — added alongside this slice's implementation,
  not part of the design decisions above.
- k8s manifests for `ingestion-service` — later slice, but `replicas: 1` is a
  constraint this design already imposes on that future manifest.
- Angular frontend.
