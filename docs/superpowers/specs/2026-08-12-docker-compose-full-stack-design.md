# Design: full-stack `docker-compose` — the whole pipeline in containers

Status: Approved
Branch: `feature/docker-compose-full-stack`

## Context

Follow-on slice to `k8s` (see
[2026-08-11-k8s-design.md](2026-08-11-k8s-design.md)). Per the chosen architecture (see
[README.md](../../../README.md)):

```
Input.txt --> ingestion-service --> Kafka topic --> processing-service --> REST API --> frontend
                                                          (Kafka Streams          (Angular)
                                                           aggregation)
```

Today there are exactly two ways to run this project, and neither is "the whole stack in
containers, in one command":

- **`docker compose up -d`** starts a Kafka broker and nothing else. Both
  [`ingestion-service/README.md`](../../../ingestion-service/README.md) and
  [`processing-service/README.md`](../../../processing-service/README.md) document it as
  the first step of a dev loop where the services themselves then run on the host via
  `mvn spring-boot:run`.
- **`kind` + `kubectl apply -f k8s/`** runs everything in containers, but requires a
  Kubernetes cluster, three `kind load docker-image` steps, and two `kubectl port-forward`
  invocations.

This slice adds the missing middle: the full pipeline in containers with no Kubernetes.

**The open problem this design exists to solve:** the `frontend` image cannot currently
run outside Kubernetes at all. `frontend/nginx.conf.template` hardcodes its upstream as
`http://processing-service.pfm.svc.cluster.local:8082` — a Kubernetes-internal DNS name
that does not resolve under Docker Compose. The obvious fix (use the short name
`processing-service`, which Docker's embedded DNS resolves) breaks Kubernetes, because
nginx's `resolver` directive performs its own DNS lookups and does **not** apply the
search-domain expansion that makes short names work in-cluster. The two environments
genuinely require different upstream strings, so the upstream has to become configurable
rather than baked into the image.

## Decisions made during brainstorming

1. **Layout — compose profiles in the existing `docker-compose.yml`, not a second file.**
   `kafka` stays unprofiled; `ingestion-service`, `processing-service`, `frontend`, and
   `wait-for-topic` all carry `profiles: [full]`. `docker compose up -d` therefore
   continues to start Kafka *only*, exactly as the two service READMEs already document,
   while `docker compose --profile full up -d --build` runs the entire stack.
   - **Why not a separate `docker-compose.full.yml`**: cleanest separation, but the Kafka
     service definition would be duplicated across two files and free to drift.
   - **Why not extend `docker-compose.yml` with no profiles**: simplest file, but it
     silently redefines what the READMEs' documented `docker compose up -d` does, and
     forces a full Maven image build on someone who only wanted a broker.
2. **nginx upstream — an env var substituted into the template, applied to both
   environments.** `frontend/nginx.conf.template` changes to
   `set $upstream ${PROCESSING_SERVICE_UPSTREAM};`. Compose sets
   `http://processing-service:8082`; `k8s/frontend.yaml` gains an explicit env var
   carrying the FQDN it hardcodes today. This reuses the `envsubst` templating the
   `nginx:alpine` entrypoint already performs for `${NGINX_LOCAL_RESOLVERS}` — no new
   mechanism.
   - **Failure mode is deliberately loud**: the nginx entrypoint substitutes only
     *defined* env vars, so a missing `PROCESSING_SERVICE_UPSTREAM` leaves the literal
     placeholder in the rendered config and nginx refuses to start. Preferred over an
     empty-string upstream, which would start cleanly and then emit confusing 502s.
   - **Rejected: a Docker network alias** of `processing-service.pfm.svc.cluster.local`
     on the compose service, leaving the template and the k8s manifest untouched. Works,
     and touches the fewest files, but leaves a Kubernetes FQDN sitting in a compose file
     with nothing to explain why it resolves.
   - **Rejected: an `envsubst` default value**
     (`${PROCESSING_SERVICE_UPSTREAM:-http://processing-service.pfm...}`), which would
     spare the k8s manifest edit. Rejected because it hides k8s's dependency on this
     value inside a shell-style default that reads like a literal.
3. **Startup ordering — a one-shot `wait-for-topic` service, mirroring the k8s
   `initContainer`.** `processing-service`'s Kafka Streams topology declares
   `future-transactions` as a source topic but never creates it; only `ingestion-service`
   does, via its `NewTopic` bean. Starting against a fresh broker before the topic exists
   throws a fatal `MissingSourceTopicException` that permanently kills the `StreamThread`
   (see the k8s design's context section). Compose has no `initContainer`, so a
   `wait-for-topic` service reuses the `apache/kafka:3.9.2` image and the *same* poll loop
   as `k8s/processing-service.yaml`, with `--bootstrap-server broker:19092` in place of
   the manifest's `kafka:9092`; `processing-service` gates on it with
   `depends_on: {wait-for-topic: {condition: service_completed_successfully}}`.
   - **Why not gate on `ingestion-service: condition: service_healthy`** as a proxy for
     "the topic exists": Spring's `KafkaAdmin` defaults to `fatalIfBrokerNotAvailable=false`,
     so it logs and continues when topic creation fails. `ingestion-service` can therefore
     report healthy without the topic existing, making this an indirect signal that is not
     always true. Polling the broker checks the actual precondition — the same reasoning
     the k8s design used for choosing a direct poll over waiting on `ingestion-service`'s
     readiness.
   - **Why not mirror the `NewTopic` bean into `processing-service`**, which would make
     the ordering problem disappear entirely: explicitly rejected by k8s design decision 1
     to keep `ingestion-service` the sole owner of topic creation. Taking it here would
     contradict a decision made one slice ago.
4. **Kafka's listener configuration is unchanged.** The existing dual-listener setup
   already serves both audiences correctly: `PLAINTEXT_HOST://localhost:9092` for
   host-machine clients (the `mvn spring-boot:run` dev loop) and
   `PLAINTEXT://broker:19092` for in-network containers. The three containerized services
   get `KAFKA_BOOTSTRAP_SERVERS=broker:19092`. This is the same split the k8s slice
   *removed* (k8s design decision 9) because in-cluster every client is a pod — under
   compose both audiences genuinely coexist, so the split earns its keep here.
5. **Healthcheck only on `kafka`, not on the three application services.** Nothing gates
   on the application services' readiness: `processing-service` gates on the topic, and
   `frontend` resolves its upstream lazily at request time via the `resolver` directive,
   so it needs no upstream running at start. A healthcheck on those services would be
   decoration. It would also require adding an HTTP client to
   `processing-service`'s image — `eclipse-temurin:21-jre` ships neither `curl` nor
   `wget` — a new image dependency solely to satisfy compose.
6. **Ingestion stays a manual `curl`.** `POST /api/ingest` is triggered by hand after the
   stack is up, consistent with k8s design decision 10 and with `ingestion-service`'s
   REST-triggered design (no `CommandLineRunner`, no directory watcher, no Job wrapper).
   - **Rejected: a one-shot auto-ingest `seed` service**, even behind its own profile.
     Better first-run demo, but it diverges from the k8s path and removes the ability to
     observe the frontend's `503`-retry behavior, which is real designed behavior worth
     being able to watch.
7. **Validation against a running stack, not manifest inspection.** Same standard as k8s
   design decision 13. Because decision 2 modifies a file the k8s deployment path also
   consumes, validation includes re-verifying the k8s frontend on a `kind` cluster, not
   just the compose path.

## Components

**Modified files:**

- **`docker-compose.yml`** — `kafka` gains a `healthcheck` (`kafka-topics.sh --list`
  against `localhost:9092` inside the container) and keeps no profile. Four services
  added, all with `profiles: [full]`:

  | Service | Image / build | Host port | Depends on |
  |---|---|---|---|
  | `ingestion-service` | build context `.`, `ingestion-service/Dockerfile` | 8081 | `kafka` healthy |
  | `wait-for-topic` | `apache/kafka:3.9.2`, one-shot | — | `kafka` healthy |
  | `processing-service` | build context `.`, `processing-service/Dockerfile` | 8082 | `wait-for-topic` completed successfully |
  | `frontend` | build context `frontend/` | 8080 → 80 | `processing-service` started (ordering only) |

  `frontend`'s `depends_on` is a plain `service_started` for predictable startup ordering,
  not a readiness gate — per decision 5 it does not need `processing-service` reachable to
  start serving.

  Both backend services get `KAFKA_BOOTSTRAP_SERVERS=broker:19092`; `frontend` gets
  `PROCESSING_SERVICE_UPSTREAM=http://processing-service:8082`. The backend build
  contexts are the repo root because both Dockerfiles need `common`'s sources (k8s design
  decision 7).

- **`frontend/nginx.conf.template`** — `set $upstream http://processing-service.pfm.svc.cluster.local:8082;`
  becomes `set $upstream ${PROCESSING_SERVICE_UPSTREAM};`. No other line changes; the
  `proxy_pass $upstream;` pure-passthrough form stays exactly as-is (it is load-bearing —
  see the k8s design's decision 8 correction note).

- **`k8s/frontend.yaml`** — the `frontend` container gains
  `env: [{name: PROCESSING_SERVICE_UPSTREAM, value: "http://processing-service.pfm.svc.cluster.local:8082"}]`,
  preserving today's behavior now that the value is no longer baked into the image.

- **`README.md`** — a full-stack compose section alongside the existing k8s instructions.

**Unchanged:** `ingestion-service/README.md` and `processing-service/README.md` (their
documented `docker compose up -d` dev loop still behaves identically), `k8s/README.md`,
all three Dockerfiles, and all application code.

## Data flow (startup)

1. `docker compose --profile full up -d --build` builds the three images and starts
   `kafka`.
2. `kafka` passes its healthcheck; `ingestion-service` and `wait-for-topic` both start.
3. `ingestion-service`'s Spring context boots and creates `future-transactions` via its
   existing `NewTopic` bean, independent of whether `POST /api/ingest` is ever called.
4. `wait-for-topic` polls the broker until `future-transactions` is listed, then exits 0.
5. `processing-service` starts; topology validation succeeds because the topic now
   exists, and the `StreamThread` reaches `RUNNING`.
6. `frontend` starts, serving the Angular build and reverse-proxying `/api/*` to
   `processing-service:8082`.
7. The operator runs `curl -X POST http://localhost:8081/api/ingest` to publish the
   sample data.
8. `http://localhost:8080` shows the frontend's `503`-retry banner until the state store
   is queryable, then renders the report table.

## Error handling

- **Kafka not yet accepting connections** — `ingestion-service` and `wait-for-topic` both
  gate on Kafka's healthcheck, so neither starts early. Consistent with the k8s README's
  finding that `ingestion-service`'s producer defaults (retries bounded by a 120s
  `delivery.timeout.ms`) tolerate a briefly-unavailable broker anyway.
- **Topic missing at `processing-service` start** — prevented by `wait-for-topic`. The
  poll loop has no timeout, matching the k8s initContainer; a stack stuck with
  `wait-for-topic` still running means Kafka or `ingestion-service` never came up, and
  its log line (`waiting for future-transactions topic...`) is the diagnostic.
- **`PROCESSING_SERVICE_UPSTREAM` unset** — nginx fails to start with a config error
  rather than serving a broken upstream (decision 2).
- **`processing-service` not yet `RUNNING` when the frontend loads** — already handled by
  the frontend's existing `503`-retry loop; no compose-level concern.

## Testing

- **Compose, from a clean state** (`docker compose down -v` first, so no topic
  pre-exists — the condition the ordering fix actually guards against):
  1. `docker compose --profile full up -d --build`; all five containers reach their
     expected state, `wait-for-topic` exits 0, and `processing-service`'s log shows no
     `MissingSourceTopicException`.
  2. `curl -X POST http://localhost:8081/api/ingest` reports 717 published, 0 errors.
  3. `curl http://localhost:8082/api/report/csv` matches `sample-output/Output.csv`.
  4. `http://localhost:8080` renders the report table through the nginx proxy — proving
     the rendered upstream works, which a `curl` straight to 8082 would not.
- **Profile isolation**: after `docker compose down`, a plain `docker compose up -d`
  starts the `kafka` container only.
- **k8s regression** (`kind`): rebuild and load the `frontend` image, `kubectl apply -f
  k8s/`, and confirm the frontend still renders the report — the template change is
  shared with the k8s path, so this is a regression risk, not a formality.

## Out of scope for this slice

- Automating `POST /api/ingest` in any form (decision 6).
- Persistent Kafka storage under compose — unchanged from today's ephemeral behavior,
  matching the same accepted limitation in k8s (k8s design decision 9).
- Resource limits, `securityContext`-equivalents, TLS, or authentication — the same
  limitations the k8s README already documents apply here.
- Replacing `kind` as the Kubernetes path, or unifying the two deployment descriptions
  into one tool. Compose and k8s remain two independent ways to run the stack.
- Any change to application code, the three Dockerfiles, or the two service READMEs.
