# Processed Future Movement

Daily summary reporting for futures transactions produced by System A.

## Problem

System A emits `Input.txt`, a fixed-width text file where each 176-byte record is
one future transaction for a client. The business needs a daily summary — for each
unique (client, product) pair, the net transaction quantity
(`sum(QUANTITY LONG - QUANTITY SHORT)`) across the day — delivered as `Output.csv`
and via a REST API.

Full field layout: [docs/file-spec.md](docs/file-spec.md).
Requirements source: `Requirements Specification.pdf`, `System A File Specification.pdf`.

## Architecture

This is being built as a real-time pipeline rather than a one-shot batch job:

```
Input.txt --> ingestion-service --> Kafka topic --> processing-service --> REST API --> frontend
                                                          (Kafka Streams          (Angular)
                                                           aggregation)
```

| Module | Responsibility |
|---|---|
| [`common`](common/) | Shared fixed-width parsing, domain model, Kafka message schema — used by both services so parsing logic exists exactly once |
| [`ingestion-service`](ingestion-service/) | Reads the fixed-width file and publishes one Kafka event per transaction record |
| [`processing-service`](processing-service/) | Consumes the topic, maintains a running per-(client, product) aggregate, exposes it via REST (JSON + CSV download) |
| [`frontend`](frontend/) | Angular UI to view and download the daily summary |
| [`k8s`](k8s/) | Kubernetes manifests for the whole stack |

## Running the whole stack

Everything in containers, one command (first run builds three images, so it takes a
few minutes):

```bash
docker compose up -d --build
```

Then publish the sample data and open the UI at `http://localhost:8080`:

```bash
curl -X POST http://localhost:8081/api/ingest
```

`processing-service` waits for the `future-transactions` topic to exist before starting,
so seeing `pfm-wait-for-topic` as `Exited (0)` in `docker compose ps -a` is expected, not
a failure (it's a one-shot container, so it won't show up in plain `docker compose ps`
at all). That wait loop has no timeout, so if `docker compose up -d --build` seems to
hang — `processing-service` never reports healthy — check `docker compose logs
wait-for-topic` in a second terminal to see whether it's still polling or Kafka never
came up. Tear down with `docker compose down -v`.

For the Kubernetes path instead, see [k8s/README.md](k8s/README.md).

### Just the broker

To run the services on the host via Maven and containerize only Kafka — the loop the
service READMEs describe:

```bash
docker compose up -d kafka
```

## Sample output

[`sample-output/Output.csv`](sample-output/Output.csv) is computed directly from
[`sample-data/Input.txt`](sample-data/Input.txt) (the provided sample, 717 records) so the
expected result is visible without running anything.

## Status

- `common` — done: fixed-width parser + domain model.
- `ingestion-service` — done: `POST /api/ingest` reads the file and publishes to Kafka
  (JSON, keyed by client+product, idempotent per file version, each record carrying a
  content-derived `transactionId` header for downstream dedup). See its
  [README](ingestion-service/README.md) for usage.
- `processing-service` — done: Kafka Streams consumer dedupes on `transactionId` and
  maintains a running per-(client, product) net-quantity aggregate, exposed via
  `GET /api/report` and `GET /api/report/csv`. See its
  [README](processing-service/README.md) for usage.
- `frontend` — done: Angular UI displays the daily summary report and downloads it as
  CSV, distinguishing "store not ready" (`503`, auto-retries) from "zero rows so far"
  (`200` with `[]`). See its [README](frontend/README.md) for usage.
- `k8s` — done: manifests for Kafka, `ingestion-service`, `processing-service`, and
  `frontend` in a `pfm` namespace, plus Dockerfiles for all three application images.
  `processing-service`'s Kafka Streams startup-ordering bug (fatal if it starts before
  the `future-transactions` topic exists) is fixed with a k8s-level `initContainer` that
  waits for the topic directly, not an application-code change. See its
  [README](k8s/README.md) for local kind usage.

See `CLAUDE.md` for AI-assistance context on this project.
