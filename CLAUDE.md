# CLAUDE.md

Operational context for working on this repo with Claude. Read
[README.md](README.md) first for what the project is and how it fits together —
this file covers only what isn't obvious from the code, and what's easy to break.

## Design source of truth

`docs/superpowers/specs/` holds a design doc per slice, each recording the
decisions **and the rejected alternatives with reasons**. Before changing
behaviour, read the spec for that area — several things that look like
oversights are deliberate, and the reasoning is written down.

Plans live in `docs/superpowers/plans/`.

## Build and test

Java 21, Maven multi-module (`common`, `ingestion-service`, `processing-service`).
`frontend` and `k8s` sit outside the Maven reactor.

```bash
mvn -q -DskipTests install    # build all modules, install common to ~/.m2
mvn verify                    # full suite, including Testcontainers integration tests
```

```bash
cd frontend && npm ci && npm test
```

**Do not add `-am` to `spring-boot:run`.** `-am` pulls the root aggregator POM
into the reactor and `spring-boot:run` then tries to run every reactor project,
including pom-packaged ones with no main class, failing with "Unable to find a
suitable main class". Run `install` first so `common` resolves from the local
repo, then use `-pl <module>` alone.

Testcontainers integration tests need a running Docker daemon.

## Running

```bash
docker compose up -d --build   # whole stack: kafka + both services + frontend
```

```bash
docker compose up -d kafka     # broker only, for the mvn spring-boot:run dev loop
```

```bash
docker compose down -v         # teardown, including all Kafka and state-store data
```

Frontend on 8080, ingestion-service 8081, processing-service 8082, Kafka 9092.
Kubernetes (kind) path is in [k8s/README.md](k8s/README.md).

`./sample-data` is mounted read-only into `ingestion-service`, so the input file
can be swapped on the host without rebuilding the image.

## Endpoints

| Method | Path | Service |
|---|---|---|
| POST | `/api/v1/ingest` (`?force=true`) | ingestion-service :8081 |
| GET | `/api/v1/ingest/status` | ingestion-service :8081 |
| GET | `/api/v1/report` | processing-service :8082 |
| GET | `/api/v1/report/csv` | processing-service :8082 |

## Invariants — do not break these

Each of these is load-bearing and none is self-evident from the surrounding code.

- **`Output.csv` has exactly three columns** — `Client_Information`,
  `Product_Information`, `Total_Transaction_Amount`. `ReportEntry` carries extra
  statistics for the UI; the CSV builder projects only these three, deliberately.
  A test pins the header — if it fails, the fix is the code, not the test.

- **`location = /api/v1/ingest/status` in `frontend/nginx.conf.template` is an
  exact match on purpose.** It means `POST /api/v1/ingest` does *not* match, falls
  through to `/api/` → processing-service, and 404s. The UI is a viewer and cannot
  trigger ingestion, enforced by routing rather than by convention. Changing this
  to a prefix match silently opens an unauthenticated write path.

- **`proxy_pass $upstream;` must have no URI suffix.** Combining a variable
  `proxy_pass` with a static suffix is an nginx gotcha that silently truncates
  every request to the suffix path.

- **Kafka's dual listeners are deliberate.** Containers use `broker:19092`; host
  processes use `localhost:9092`. The k8s manifests use a single listener because
  in-cluster every client is a pod — that difference is intentional, not drift.

- **`ingestion-service` is the sole topic creator**, via its `NewTopic` bean.
  `processing-service` deliberately never creates `future-transactions`; it is
  gated at deploy time instead (compose `wait-for-topic`, k8s `initContainer`).
  Without that gate it dies on a fatal `MissingSourceTopicException` that kills
  the `StreamThread` with no self-recovery. Do not "fix" this by adding a
  `NewTopic` bean to `processing-service` — that was considered and rejected.

- **`replicas: 1` on all three services, for three different reasons.**
  ingestion-service: in-process idempotency cache. processing-service: Interactive
  Queries need one instance owning all partitions or the report is silently
  partial. kafka: it is a Deployment, not a StatefulSet — two replicas would be
  two conflicting brokers, not a cluster.

- **The topic name is declared once.** Compose uses a YAML anchor; k8s uses the
  `pfm-topic-config` ConfigMap. Both feed `PFM_TOPIC`. Do not reintroduce a second
  literal — a mismatch between the two services fails silently, with ingestion
  publishing to one topic while processing consumes another and the report shows
  an empty table.

- **The report is a running aggregate, not a replace.** `transactionId` is
  `sha256(contentHash + ":" + lineNumber)`, so re-ingesting the *same* file is a
  no-op, but a *different* file adds to the existing totals. Run
  `docker compose down -v` before testing a different input file, or the numbers
  will look wrong.

## Assumptions baked into the code

- **`D` = debit = negative** (`FutureTransactionFactory`). All 717 sample records
  carry `D` on all three money fields and there is no `C` example, so the
  accounting convention is assumed rather than verified. This became load-bearing
  once fees were surfaced in the UI — fee totals display as negative, which is
  expected, not a bug.
- Quantity signs: blank or `+` is positive, `-` negates.
- Records are 176 bytes with trailing FILLER stripped; the spec says 303.
- `sample-output/Output.csv` is the reference truth for the sample input, and the
  full-pipeline golden test asserts against it.

## Workflow used

Built with the `superpowers` skills: brainstorm → spec → plan → implement, one
slice at a time, with the spec and plan committed before implementation starts.
The specs in `docs/superpowers/specs/` are the record of that process, including
the alternatives considered and why they were rejected.

When picking up new work here, follow the same loop rather than editing directly:
invoke `superpowers:brainstorming` first for anything that changes behaviour.
