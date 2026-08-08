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

Local dev stack (Kafka, etc.) is defined in `docker-compose.yml` (added alongside the
service implementations).

## Sample output

[`sample-output/Output.csv`](sample-output/Output.csv) is computed directly from
[`sample-data/Input.txt`](sample-data/Input.txt) (the provided sample, 717 records) so the
expected result is visible without running anything.

## Status

Repo structure and sample output only so far — service implementations are in progress.
See `CLAUDE.md` for AI-assistance context on this project.
