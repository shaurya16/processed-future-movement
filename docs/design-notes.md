# Design notes

Why the system looks the way it does. For how it fits together, see
[architecture](architecture.md). For the overview and how to run it, see the
[README](../README.md).

## Why ingestion is REST-triggered

System A writes its file to a configured location (`INGESTION_FILE_PATH`). In a deployed
environment, a cron job or scheduler drives `POST /api/v1/ingest` on whatever cadence the
business needs. **The point is that scheduling policy lives outside the service.**

[Design decision 1 of the ingestion-service spec](superpowers/specs/2026-08-09-ingestion-service-design.md)
records the two rejected alternatives:

- **A `CommandLineRunner`** blurs "service" with "one-shot job", and cannot be re-run
  without restarting the process.
- **A directory watcher** needs more infrastructure than a single daily file justifies.

A REST trigger makes the service schedulable by anything — cron, an orchestrator, a
manual `curl`, a CI job — without the service itself owning that concern. It is also what
makes the endpoint testable and re-runnable, which the full-pipeline golden test depends on.

## Why the startup gate exists

Kafka Streams validates its topology at startup. If the source
topic does not exist yet, it throws a fatal `MissingSourceTopicException` that kills the
`StreamThread` — and the thread does not recover on its own. The application process
stays up while serving nothing, which is worse than crashing. Gating the start on the
topic's existence removes the race entirely, without an application-code retry loop that
would only paper over it.

## Why `503` and not an empty `200`

Between process start and the Kafka Streams store
reaching `RUNNING`, the store cannot be queried. Returning `200` with `[]` would make
"not ready yet" indistinguishable from "genuinely zero rows" — a client polling during
startup would render an empty report and look correct while being wrong. `503` says the
resource exists but is not available yet, which is exactly the situation, and lets the
frontend retry rather than display nothing. The frontend keeps the last good data on a
failed refresh for the same reason.

## CSV vs JSON: a deliberate divergence

`Output.csv` carries **exactly the three required columns** — `Client_Information`,
`Product_Information`, `Total_Transaction_Amount` — and nothing else. That contract is
frozen and pinned by the golden test.

`GET /api/v1/report` returns more: the same three fields under the same names, plus the
decomposed key dimensions the UI filters and sorts on (client type, account number,
exchange code, symbol, expiration date…) and additional measures that make a row
interpretable — gross long, gross short, trade count, and first and last transaction dates.
A net quantity of zero is ambiguous without the gross figures; the UI needs to show the
difference between "no activity" and "bought and sold in equal measure".

`feesByCurrency` is the one field the UI does *not* read: the Fees KPI tile was removed
(it reconciled poorly against a running aggregate) but the field was deliberately kept on
the API, since it is the only place the parsed fee data surfaces at all. Its totals come
back negative — see the `D` = debit assumption.

The CSV has not drifted from the spec. The JSON is additive. See the
[UI redesign design doc](superpowers/specs/2026-08-12-ui-tailwind-redesign-design.md).

## Why re-ingesting a different file adds to the totals

This is designed behaviour — it is the same property that makes re-ingesting the *same*
file a no-op — but it surprises people, so it is worth stating plainly.

The mechanism is the content-derived `transactionId`:
[architecture](architecture.md#transaction-ids). The operational warning is in the
[README](../README.md#using-your-own-file).

## Assumptions in depth

The assumptions themselves are tabled in the [README](../README.md#assumptions). This is
the one that carries weight:

The `D` = debit assumption is the one to watch. It is confirmed *consistent* across the
sample — all 717 records carry `D` at positions 86, 102 and 118 — but not *verified*,
because there is no `C` example anywhere in the sample, so the accounting convention is
inferred rather than observed. It decides the sign of the per-currency fee totals in the
`/api/v1/report` response; the net quantities the report table and the CSV display are
unaffected by it.

## Scalability

**Current limits, and why each exists.** `ingestion-service` runs at `replicas: 1` because
it holds an in-process idempotency cache — though that is now largely an optimisation
rather than a correctness mechanism, since `processing-service` dedupes independently on
the content-derived `transactionId`. `processing-service` runs at `replicas: 1` for a
harder reason: Interactive Queries only see state owned by the local instance, so a second
replica would split the store across instances and the report would go silently partial
rather than failing loudly. `kafka` runs at `replicas: 1` because it is a `Deployment`, not
a `StatefulSet` — two replicas would be two conflicting brokers with the same node id, not
a cluster. Beyond replication: the dedup store has no TTL, the file is parsed entirely into
memory, and sends are one blocking round-trip per record.

**What would change at 100x**, in priority order:

1. **Async batched sends** — biggest gain for the smallest change. Ordering is preserved
   because `enable.idempotence=true` is already set.
2. **Streaming parse**, so file size stops bounding heap.
3. **Partition sizing**, done deliberately and early — raising the partition count later
   rehashes keys and breaks state affinity.
4. **A TTL on the dedup store.** The real cost is changelog restore time during rebalances,
   not disk.
5. **A query strategy** — cross-instance query routing, or a CQRS read-side — which is what
   unblocks `processing-service` from `replicas: 1`.
6. **An async job model for ingestion**, so a large file does not hold an HTTP request open.

**Properties the design already has.** The message key makes aggregation partition-local,
so there is no repartition step to scale. The content-derived `transactionId` makes a full
reset-and-replay reproduce identical results, and makes range-sharding a large file across
several ingestion workers safe without any coordination between them. State stores are
persistent and changelog-backed, so an instance rebuilds rather than recomputes.

Depth on all of this lives in the
[processing-service design doc](superpowers/specs/2026-08-09-processing-service-design.md).

## The per-slice design docs

Each slice was designed before it was built. These docs carry the reasoning — the
alternatives considered and why they were rejected — and are the best place to understand
why the system looks the way it does.

| Doc | What it decides |
|---|---|
| [`common` — fixed-width parser](superpowers/specs/2026-08-09-common-fixed-width-parser-design.md) | Parsing strategy, the domain model, and why parsing lives in a shared module rather than in either service |
| [`ingestion-service`](superpowers/specs/2026-08-09-ingestion-service-design.md) | The REST trigger over a `CommandLineRunner` or directory watcher; content-derived `transactionId`s; per-file-version idempotency |
| [`processing-service`](superpowers/specs/2026-08-09-processing-service-design.md) | Kafka Streams over a plain consumer; Processor API for dedup; Interactive Queries for the report; the `replicas: 1` constraint |
| [`frontend`](superpowers/specs/2026-08-11-frontend-design.md) | Angular with signals and no component library; the polling model; CSV download via the API rather than client-side generation |
| [`k8s`](superpowers/specs/2026-08-11-k8s-design.md) | Manifest layout; the `initContainer` fix for the Streams startup race rather than an application-code retry |
| [API versioning](superpowers/specs/2026-08-12-api-versioning-design.md) | Introducing `/api/v1` while there is still one client, to avoid a breaking migration later |
| [Docker Compose full stack](superpowers/specs/2026-08-12-docker-compose-full-stack-design.md) | The whole pipeline in one command; the `wait-for-topic` gate; nginx as the single ingress |
| [Kafka topic config](superpowers/specs/2026-08-12-kafka-topic-config-design.md) | Single-sourcing the topic name that four separate files previously had to agree on |
| [UI redesign](superpowers/specs/2026-08-12-ui-tailwind-redesign-design.md) | Enriching the report contract so the UI can filter and sort, while keeping the CSV frozen at three columns |
