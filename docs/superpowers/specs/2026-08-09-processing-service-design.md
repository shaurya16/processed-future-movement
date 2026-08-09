# Design: `processing-service` — idempotent Kafka Streams aggregation + report API

Status: Approved
Branch: `feature/processing-service`

## Context

Third buildable slice of processed-future-movement, following `common` and
`ingestion-service` (see
[2026-08-09-ingestion-service-design.md](2026-08-09-ingestion-service-design.md)). Per
the chosen architecture (see [README.md](../../../README.md)):

```
Input.txt --> ingestion-service --> Kafka topic --> processing-service --> REST API --> frontend
                                                          (Kafka Streams
                                                           aggregation)
```

`processing-service` consumes `future-transactions`, maintains a running
per-(Client_Information, Product_Information) net-quantity aggregate
(`sum(quantityLong - quantityShort)`), and exposes it via `GET /api/report` (JSON) and
`GET /api/report/csv`.

**The open problem this design exists to solve:** `ingestion-service`'s known
architectural trade-off (see project status memory) means a persistently-partially-
failing file gets fully re-parsed and re-published — including already-succeeded
records — on every non-forced retry, and `POST /api/ingest?force=true` deliberately
bypasses ingestion-service's own idempotency cache. Naively summing every message that
arrives on the topic would double-count. `processing-service`'s aggregation must treat
"this exact record, republished" as a no-op, while still correctly counting two
genuinely distinct transactions that happen to share identical field values (a client
legitimately trading the same size twice is normal business data, not a duplicate).

## Decisions made during brainstorming

1. **Dedup key — content-derived transaction ID, not a hash of the record's field
   values.** Two genuinely distinct, legitimate transactions can have identical values
   in every field of `FutureTransaction` (same client, same product, same quantity —
   plausible for a client trading the same size twice in a day). Hashing the record's
   *content* would collapse these into "the same transaction," silently undercounting
   real business data — a worse failure than the duplicate-publish problem this design
   is solving. Instead, the transaction ID is derived from **where the record occurs**:
   `transactionId = SHA-256(fileContentHash + ":" + lineNumber)`, where
   `fileContentHash = SHA-256(raw file bytes)`. This is stable across re-ingestion of
   *unchanged* file content (a retry-after-partial-failure or an explicit `force=true`
   re-run reproduces identical IDs, so `processing-service` correctly treats them as
   no-ops) while remaining distinct per line, so field-value collisions between
   different lines never cause an incorrect dedup match. A file whose *content*
   actually changes produces entirely new transaction IDs and correctly adds to the
   running total — consistent with "maintains a running aggregate," not "replaces the
   aggregate with the latest file."
   - **Rejected: ticket-number-based natural key.** Checked directly against
     `sample-data/Input.txt`: `TICKET NUMBER` is blank or `"0"` for nearly every one of
     the 716 sample records (only 2 distinct values across the whole file), so it
     carries no real per-record identity in this data.
   - **Rejected: rely on producer `acks=all` + `enable.idempotence=true` alone.**
     Idempotent producer config only dedupes retries of the *same* send call at the
     Kafka client/broker level. It does not — and cannot — cover ingestion-service's
     actual failure mode: a brand-new `POST /api/ingest` run that re-parses the file
     and issues entirely new produce calls for already-succeeded records.
   - **Emergent property, called out explicitly rather than treated as a side effect:**
     because the ID is content-derived, `force=true` no longer means "deliberately
     double-count downstream" — it now means "bypass ingestion-service's own cache and
     re-publish, which processing-service will still correctly no-op on if nothing in
     the file changed." This is a (desirable) behavior shift from how `force=true` was
     originally scoped in the ingestion-service design, and is worth knowing if anyone
     revisits that doc.
2. **Wire format — Kafka message header, not a value envelope or a field on
   `FutureTransaction`.** `transactionId` is attached as a Kafka header
   (`transactionId`, UTF-8 bytes) on each `ProducerRecord`, alongside the existing key
   and value. `common`'s `FutureTransaction` domain record and its JSON schema are
   completely untouched by this slice.
   - **Rejected: envelope type** (e.g. `FutureTransactionEvent(transactionId,
     transaction)` as the published value) — would have made the ID part of the
     documented, inspectable message schema, but rejected in favor of the header
     approach to avoid changing the value wire format/JSON shape both services already
     depend on.
   - **Rejected: field on `FutureTransaction` itself** — would pollute a pure
     business-domain record (parsed straight from System A's file spec) with an
     ingestion-technical concern unrelated to the file layout.
3. **`common` changes required to make the ID computable.** A successfully-parsed
   record's original line number is currently discarded by
   `FutureTransactionParser.parseAll()` — `ParseResult.records()` is a flat
   `List<FutureTransaction>` with no line association. Since computing
   `transactionId` needs that line number, `common` gets a new
   `record ParsedRecord(int lineNumber, FutureTransaction transaction)`, and
   `ParseResult.records()` changes type to `List<ParsedRecord>`. This is a breaking
   change to an already-merged, already-tested type, rippling into
   `FutureTransactionParserTest`, the golden test, `IngestionService`,
   `IngestionServiceTest`, and `IngestionEndToEndTest` (each needs a one-line
   `.transaction()` unwrap or `.stream().map(...)`).
   - **Rejected: re-implement skip-and-collect locally in `IngestionService`** instead
     of changing `common`, to track line numbers without touching a shared/tested
     type. Rejected because it duplicates `parseAll`'s loop-and-catch logic in two
     places that would need to stay in sync.
4. **`ingestion-service` changes.** New `ContentHash.compute(path)` — SHA-256 over raw
   file bytes via `java.security.MessageDigest` (no new dependency) — deliberately
   separate from the existing `FileFingerprint` (path+size+mtime), which is unchanged
   and keeps doing its current job (the `POST /api/ingest` idempotency-cache key). For
   each `ParsedRecord`, `IngestionService` computes `transactionId` and attaches it as
   a header on the `ProducerRecord` sent via `KafkaTemplate`.
5. **Dedup mechanism in `processing-service` — Processor API, not plain DSL.** Kafka
   Streams' DSL operators (`map`, `filter`, `groupByKey`, ...) don't expose message
   headers. A custom `Processor` reads the `transactionId` header off each record via
   the Processor API's `Record<K, V>` and checks/updates a state store before deciding
   whether to forward the record downstream.
6. **State store persistence — RocksDB/changelog-backed, not in-memory, for both
   stores.** This is the default Kafka Streams behavior for `Materialized.as(name)`
   without opting into an in-memory store, so it costs no extra code. Deliberate,
   not incidental: an in-memory dedup store would forget every `transactionId` on
   restart, so a retry-republish arriving after a restart would double-count —
   defeating the point of this whole design. Persistent stores replay their changelog
   topic on startup and resume exactly where they left off.
   - **Documented limitation:** `seen-transaction-ids` has no TTL/pruning, so it grows
     unboundedly with total records ever ingested. Fine at this project's scale (716
     sample records); the same pragmatic call `IngestionRegistry`'s unbounded
     in-memory map already makes for `ingestion-service`.
7. **Single instance.** Like `ingestion-service`, the eventual k8s manifest for
   `processing-service` needs `replicas: 1`. Interactive Queries against a partitioned
   state store need either one instance owning all partitions or a cross-instance RPC
   query-routing layer; the latter is out of scope for this project's size.
8. **Report field naming mirrors the spec exactly, not Java camelCase.** Both
   `GET /api/report` JSON keys and the CSV header use `Client_Information`,
   `Product_Information`, `Total_Transaction_Amount` — the exact names from
   `docs/file-spec.md` / `sample-output/Output.csv` — via `@JsonProperty` on an
   internal camelCase Java record. One wire contract, no translation table needed by
   the frontend later.
9. **Deterministic response ordering.** Both endpoints sort by `Client_Information`
   then `Product_Information` before responding, rather than relying on the
   underlying RocksDB store's iteration order (which happens to be lexicographic by
   serialized key today, but that's an implementation detail of the store, not a
   guarantee worth depending on).
10. **Store-not-ready handling.** If the Kafka Streams app isn't in `RUNNING` state
    (startup/rebalance), the state store is unqueryable. Both endpoints return `503`
    rather than silently returning an empty report, so "not ready yet" is never
    confused with "genuinely zero data so far."
11. **CSV built manually, no new dependency.** `Client_Information` and
    `Product_Information` are fixed concatenations of positional codes and can't
    contain commas or quotes, so manual string joining is sufficient — no CSV library
    needed for this field set.
12. **Testing includes a full cross-service golden test.** In addition to
    `processing-service`'s own Testcontainers e2e test (hand-constructed messages/
    headers), one test spins up *both* real Spring Boot services against a shared
    Testcontainers Kafka broker, drives the real `POST /api/ingest` against
    `sample-data/Input.txt`, and asserts `processing-service`'s
    `GET /api/report/csv` output matches `sample-output/Output.csv` exactly. Chosen
    over skipping cross-service validation for this slice because it's the strongest
    available correctness signal (the entire parse → publish → dedup → aggregate →
    report path, validated against the one known-correct answer for the real sample
    data) — worth the extra implementation-plan work of wiring two contexts against
    one broker in a single test module.

## Components

**`common` (modified)**
- **`ParsedRecord`** — new `record ParsedRecord(int lineNumber, FutureTransaction transaction)`.
- **`ParseResult`** — `records()` changes from `List<FutureTransaction>` to `List<ParsedRecord>`.
- **`FutureTransactionParser.parseAll()`** — wraps each successfully-parsed record as
  `new ParsedRecord(lineNumber, transaction)` instead of the bare `FutureTransaction`.

**`ingestion-service` (modified)**
- **`ContentHash`** — new utility, `compute(Path path)` returns hex-encoded SHA-256 of
  the raw file bytes.
- **`IngestionService`** — computes `contentHash` once per ingestion run; for each
  `ParsedRecord`, computes `transactionId = sha256Hex(contentHash + ":" + lineNumber)`
  and attaches it as a `transactionId` header (UTF-8 bytes) on the `ProducerRecord`
  before sending. Unwraps `ParsedRecord.transaction()` where the current code uses the
  bare `FutureTransaction`.

**`processing-service` (new module)**
- **`DedupProcessor`** — Processor API `Processor<String, FutureTransaction, String,
  FutureTransaction>`. Reads the `transactionId` header off each incoming record;
  looks it up in the `seen-transaction-ids` store; if present, drops the record
  (no forward); if absent, `put(transactionId, firstSeenEpochMillis)` and forwards the
  record unchanged.
- **`AggregationTopology`** — `@Configuration` building the `StreamsBuilder` topology:
  source (`Consumed.with(Serdes.String(), transactionJsonSerde)`) → `.process(
  DedupProcessor::new, "seen-transaction-ids")` → `.groupByKey()` → `.aggregate(() ->
  0L, (key, txn, total) -> total + (txn.quantityLong() - txn.quantityShort()),
  Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("net-quantity-store"))`.
- **`ReportEntry`** — response DTO: `clientInformation`, `productInformation`,
  `netQuantity` fields, `@JsonProperty("Client_Information")` /
  `@JsonProperty("Product_Information")` / `@JsonProperty("Total_Transaction_Amount")`
  respectively.
- **`ReportService`** — Interactive-Query access to `net-quantity-store`: checks
  `KafkaStreams.state() == RUNNING` (throws a `StoreNotReadyException` mapped to 503
  otherwise), iterates the store's `all()`, splits each key on the first `|` into
  `Client_Information` / `Product_Information`, builds `ReportEntry` list sorted by
  (`Client_Information`, `Product_Information`).
- **`ReportController`** — `GET /api/report` returns `List<ReportEntry>` as JSON;
  `GET /api/report/csv` builds the CSV body (header row + one row per entry, plain
  integer `netQuantity`, no thousands separators) with `Content-Type: text/csv` and
  `Content-Disposition: attachment; filename="Output.csv"`.
- **Kafka Streams config** — `application.yml` wiring
  `spring.kafka.streams.application-id`, bootstrap servers, and a `JsonSerde` for
  `FutureTransaction` values (mirrors `ingestion-service`'s Jackson config: registered
  `JavaTimeModule`, dates not written as timestamps) consistent with what
  `ingestion-service` actually publishes.

## Data flow

1. `ingestion-service` (already covered by its own design/tests, extended here) reads
   `Input.txt`, computes `contentHash`, and for each `ParsedRecord` publishes to
   `future-transactions` keyed by `Client_Information|Product_Information`, value =
   `FutureTransaction` JSON, header `transactionId = sha256Hex(contentHash + ":" +
   lineNumber)`.
2. `processing-service`'s Kafka Streams topology consumes the topic. The source node
   deserializes each value into `FutureTransaction`; headers pass through untouched.
3. `DedupProcessor` checks `seen-transaction-ids` for the record's `transactionId`.
   Already seen → dropped, no aggregation impact. Not seen → recorded, forwarded.
4. Forwarded records are grouped by their existing key and aggregated into
   `net-quantity-store`, a persistent `KTable<String, Long>` of running net quantity
   per (Client_Information, Product_Information).
5. `GET /api/report` / `GET /api/report/csv` query `net-quantity-store` via Interactive
   Queries (only when `KafkaStreams` state is `RUNNING`), sort, and render as
   JSON/CSV respectively.

## Testing

- **`common`**: update `FutureTransactionParserTest` and the golden test for the
  `ParsedRecord` wrapper.
- **`ingestion-service`**: unit tests for `ContentHash` (same content → same hash;
  changed content → different hash); unit tests asserting `transactionId` is stable
  across repeat ingestion of unchanged content, including via `force=true`; update
  `IngestionServiceTest`/`IngestionEndToEndTest` to assert the `transactionId` header
  is present, non-empty, and stable across two ingestion runs.
- **`processing-service` topology** (`TopologyTestDriver`): net-quantity arithmetic is
  correct for a simple multi-record case; the *same* `transactionId` sent twice only
  counts once (the actual retry-republish scenario this design exists for); two
  records with *different* `transactionId`s but identical field values both count
  (guards against the content-hash pitfall ruled out in decision 1).
- **`processing-service` controller** (mocked `ReportService`): JSON shape and field
  names, CSV header/formatting/sort order, `503` when the store isn't ready.
- **`processing-service` end-to-end** (Testcontainers Kafka, real Spring context):
  test publishes messages directly with hand-constructed `transactionId` headers
  (no dependency on `ingestion-service`), including a duplicate-header replay, and
  asserts `GET /api/report` / `GET /api/report/csv` reflect the deduped, aggregated
  result.
- **Full-pipeline golden test** (Testcontainers Kafka shared by both services' real
  Spring contexts): drives real `POST /api/ingest` against
  `sample-data/Input.txt`, then asserts `processing-service`'s
  `GET /api/report/csv` output matches `sample-output/Output.csv` exactly (after
  accounting for the sorted response order this design imposes, which the sample file
  does not appear to follow — the test compares as sets/sorted, not a raw byte diff,
  unless the sample happens to already match).

## Out of scope for this slice

- Angular frontend (next slice after this).
- k8s manifests (later slice), though `replicas: 1` is a constraint this design
  already imposes on that future manifest, same as `ingestion-service`.
- TTL/pruning for the `seen-transaction-ids` store — documented limitation, not solved
  here (see decision 6).
- Cross-instance query routing for Interactive Queries — not needed under the
  `replicas: 1` constraint.
