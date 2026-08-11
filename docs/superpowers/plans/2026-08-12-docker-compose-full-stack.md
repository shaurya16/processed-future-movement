# Full-Stack docker-compose Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `docker compose up -d --build` bring up the entire pipeline — Kafka, `ingestion-service`, `processing-service`, and `frontend` — in containers, with no Kubernetes.

**Architecture:** All five services live unprofiled in the existing `docker-compose.yml`. Kafka gains a healthcheck; a one-shot `wait-for-topic` service mirrors the k8s `initContainer` that guards `processing-service`'s fatal startup-ordering bug. The `frontend` image's nginx upstream, currently a hardcoded Kubernetes FQDN, becomes an env var so the same image works under both Compose and k8s.

**Tech Stack:** Docker Compose v2 (`service_healthy` / `service_completed_successfully` conditions), `apache/kafka:3.9.2`, nginx `envsubst` templating, Spring Boot, Kafka Streams.

**Spec:** [docs/superpowers/specs/2026-08-12-docker-compose-full-stack-design.md](../specs/2026-08-12-docker-compose-full-stack-design.md)

## Global Constraints

- Branch: `feature/docker-compose-full-stack`. All commits land here.
- **No application code changes.** Java sources, the three Dockerfiles, and `k8s/README.md` are untouched by this plan.
- Kafka's listener configuration in `docker-compose.yml` is **unchanged**. Containers reach the broker at `broker:19092`; host clients keep using `localhost:9092`.
- Host port mapping: `frontend` 8080→80, `ingestion-service` 8081, `processing-service` 8082, `kafka` 9092.
- Image tags: `pfm/ingestion-service:local`, `pfm/processing-service:local`, `pfm/frontend:local` — the same tags `k8s/README.md` already documents.
- The `wait-for-topic` poll loop is copied from `k8s/processing-service.yaml`'s `initContainer` verbatim except for the bootstrap address, which becomes `broker:19092`.
- Scratchpad for throwaway files: `/private/tmp/claude-501/-Users-shaurya-Documents-Dev-processed-future-movement/a9e51102-0a10-4acb-bcd6-470170c27c86/scratchpad`

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `frontend/nginx.conf.template` | Modify (1 line) | SPA serving + `/api` reverse proxy; upstream now injected via env |
| `k8s/frontend.yaml` | Modify (add `env:`) | Supplies the k8s FQDN now that it is no longer baked into the image |
| `docker-compose.yml` | Modify (healthcheck + 4 services) | Single-command local stack |
| `README.md` | Modify | Documents the full-stack command |
| `ingestion-service/README.md` | Modify (1 line) | Dev loop moves to `docker compose up -d kafka` |
| `processing-service/README.md` | Modify (1 line) | Dev loop moves to `docker compose up -d kafka` |

---

### Task 1: Parameterize the nginx upstream

The `frontend` image currently cannot run outside Kubernetes: its upstream is the hardcoded FQDN `http://processing-service.pfm.svc.cluster.local:8082`. This task makes it configurable and proves the k8s value still flows through.

**Files:**
- Modify: `frontend/nginx.conf.template:8`
- Modify: `k8s/frontend.yaml` (add `env:` to the `frontend` container)

**Interfaces:**
- Consumes: nothing (first task).
- Produces: env var **`PROCESSING_SERVICE_UPSTREAM`**, a full scheme+host+port URL string with no trailing path (e.g. `http://processing-service:8082`). Task 2 sets it in `docker-compose.yml`; Task 4 verifies the k8s value.

- [ ] **Step 1: Write the failing test — confirm the env var is currently ignored**

Build the image as it stands and start it with a sentinel upstream:

```bash
docker build -t pfm/frontend:local frontend/ \
  && docker rm -f fe-probe 2>/dev/null; \
  docker run -d --name fe-probe -e PROCESSING_SERVICE_UPSTREAM=http://sentinel:9999 pfm/frontend:local \
  && sleep 3 && docker exec fe-probe cat /etc/nginx/conf.d/default.conf
```

- [ ] **Step 2: Verify it fails**

Expected: the rendered config still contains `set $upstream http://processing-service.pfm.svc.cluster.local:8082;` and does **not** mention `sentinel`. That is the failure — the env var has no effect.

Clean up:

```bash
docker rm -f fe-probe
```

- [ ] **Step 3: Make the upstream configurable**

In `frontend/nginx.conf.template`, change exactly one line inside `location /api/`:

```
        set $upstream ${PROCESSING_SERVICE_UPSTREAM};
```

Leave every other line untouched. In particular `proxy_pass $upstream;` must keep its bare, no-URI-suffix form — adding a `/api/` suffix to a variable `proxy_pass` silently truncates every request (documented in the k8s spec, decision 8). The full file afterwards:

```
server {
    listen 80;
    root /usr/share/nginx/html;
    index index.html;
    resolver ${NGINX_LOCAL_RESOLVERS};

    location /api/ {
        set $upstream ${PROCESSING_SERVICE_UPSTREAM};
        proxy_pass $upstream;
        proxy_set_header Host $host;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
docker build -t pfm/frontend:local frontend/ \
  && docker rm -f fe-probe 2>/dev/null; \
  docker run -d --name fe-probe -e PROCESSING_SERVICE_UPSTREAM=http://sentinel:9999 pfm/frontend:local \
  && sleep 3 && docker exec fe-probe cat /etc/nginx/conf.d/default.conf; \
  docker rm -f fe-probe
```

Expected: the rendered config now reads `set $upstream http://sentinel:9999;`, and the k8s FQDN no longer appears anywhere.

- [ ] **Step 5: Verify the missing-env-var failure is loud**

```bash
docker run --rm pfm/frontend:local
```

Expected: the container exits non-zero with an nginx emerg line naming the variable, e.g.
`nginx: [emerg] unknown "PROCESSING_SERVICE_UPSTREAM" variable`.

This is the designed behaviour (spec decision 2): unsubstituted `${...}` is read by nginx as a variable reference, so a missing env var stops the container at startup instead of silently producing an empty upstream and confusing 502s later.

- [ ] **Step 6: Restore the k8s value via the manifest**

In `k8s/frontend.yaml`, add an `env:` block to the `frontend` container, directly after its `ports:` block:

```yaml
          ports:
            - containerPort: 80
          env:
            - name: PROCESSING_SERVICE_UPSTREAM
              value: "http://processing-service.pfm.svc.cluster.local:8082"
```

Without this the k8s deployment breaks, because the value it used to get from the image is gone. Task 4 verifies this on a real cluster.

- [ ] **Step 7: Commit**

```bash
git add frontend/nginx.conf.template k8s/frontend.yaml
git commit -m "refactor(frontend): make nginx upstream configurable via env var

The upstream was a hardcoded Kubernetes FQDN, so the image could not run
under Docker Compose. The short name that Docker resolves does not work in
Kubernetes either, because nginx's resolver skips search-domain expansion —
so the value has to come from the environment. k8s/frontend.yaml now supplies
the FQDN it previously got from the image.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: Add the full stack to docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: `PROCESSING_SERVICE_UPSTREAM` from Task 1.
- Produces: services `kafka`, `ingestion-service`, `wait-for-topic`, `processing-service`, `frontend`. Task 3 documents `docker compose up -d --build` and `docker compose up -d kafka`.

- [ ] **Step 1: Write the failing test — a cold start with no topic**

Tear everything down so no `future-transactions` topic exists. This is the exact condition the ordering fix guards against; testing against a warm broker proves nothing.

```bash
docker compose down -v --remove-orphans
docker compose ps
```

Expected: no containers listed.

- [ ] **Step 2: Verify it fails**

```bash
docker compose up -d
docker compose ps
```

Expected: only `pfm-kafka` starts — `docker-compose.yml` defines nothing else yet. There is no `processing-service`, so `curl http://localhost:8082/api/report` fails to connect. That is the gap this task closes.

```bash
docker compose down -v
```

- [ ] **Step 3: Add the healthcheck to the kafka service**

In `docker-compose.yml`, append a `healthcheck` to the existing `kafka` service, after its `environment:` block. Change nothing else about `kafka` — the dual-listener config is deliberate and load-bearing.

```yaml
    healthcheck:
      test: ['CMD', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--list']
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 20s
```

`localhost:9092` is correct here: it is the `PLAINTEXT_HOST` listener, reachable from inside the Kafka container itself.

- [ ] **Step 4: Add the four remaining services**

Append to `docker-compose.yml`, at the same indentation level as `kafka` (two spaces under `services:`):

```yaml
  ingestion-service:
    build:
      context: .
      dockerfile: ingestion-service/Dockerfile
    image: pfm/ingestion-service:local
    container_name: pfm-ingestion-service
    ports:
      - '8081:8081'
    environment:
      KAFKA_BOOTSTRAP_SERVERS: 'broker:19092'
    depends_on:
      kafka:
        condition: service_healthy

  wait-for-topic:
    image: apache/kafka:3.9.2
    container_name: pfm-wait-for-topic
    depends_on:
      kafka:
        condition: service_healthy
    command:
      - sh
      - -c
      - |
        until /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:19092 --list | grep -qx future-transactions; do
          echo "waiting for future-transactions topic..."
          sleep 2
        done
        echo "future-transactions topic found, starting processing-service"

  processing-service:
    build:
      context: .
      dockerfile: processing-service/Dockerfile
    image: pfm/processing-service:local
    container_name: pfm-processing-service
    ports:
      - '8082:8082'
    environment:
      KAFKA_BOOTSTRAP_SERVERS: 'broker:19092'
    depends_on:
      wait-for-topic:
        condition: service_completed_successfully

  frontend:
    build:
      context: frontend
    image: pfm/frontend:local
    container_name: pfm-frontend
    ports:
      - '8080:80'
    environment:
      PROCESSING_SERVICE_UPSTREAM: 'http://processing-service:8082'
    depends_on:
      - processing-service
```

Three things that are deliberate and must not be "tidied":

- Both backend `build.context` values are `.` (the repo root), not the module directory. Their Dockerfiles need `common`'s sources, which live outside the module. The `frontend` context is `frontend` because its Dockerfile is self-contained.
- `wait-for-topic` gates only on `kafka` being healthy, not on `ingestion-service`. It polls for the real precondition — the topic existing — which `ingestion-service` creates during its own startup. Gating on `ingestion-service`'s health instead would be an indirect signal that can be true while the topic is still absent.
- `frontend`'s `depends_on` is the plain list form (`service_started`), purely for startup ordering. nginx resolves its upstream lazily at request time, so it does not need `processing-service` reachable to boot.

- [ ] **Step 5: Validate the compose file parses**

```bash
docker compose config --quiet && echo "compose file OK"
```

Expected: `compose file OK`, no YAML or schema errors.

- [ ] **Step 6: Bring up the full stack from cold**

```bash
docker compose up -d --build
```

Expected: three images build (the two Maven builds take several minutes on a cold cache), then all services start. `wait-for-topic` runs and exits.

- [ ] **Step 7: Verify every container reached its expected state**

```bash
docker compose ps -a
```

Expected: `pfm-kafka` (healthy), `pfm-ingestion-service`, `pfm-processing-service`, `pfm-frontend` all `Up`; `pfm-wait-for-topic` `Exited (0)`.

- [ ] **Step 8: Verify the ordering bug did not fire**

```bash
docker compose logs wait-for-topic
docker compose logs processing-service | grep -i 'MissingSourceTopic' && echo "BUG PRESENT" || echo "no MissingSourceTopicException"
```

Expected: `wait-for-topic` logs its `future-transactions topic found` line, and the grep prints `no MissingSourceTopicException`. If `MissingSourceTopicException` appears, the gate did not work — stop and fix before continuing.

- [ ] **Step 9: Ingest the sample data**

```bash
curl -s -X POST http://localhost:8081/api/ingest | tee /dev/stderr | grep -q '"published":717' && echo "717 published"
```

Expected: JSON body with `"totalLines":717`, `"published":717`, `"errors":[]`, `"cached":false`, and the `717 published` confirmation.

- [ ] **Step 10: Verify the report matches the known-correct output**

Kafka Streams needs a moment to consume and aggregate, so allow a short settle.

```bash
SCRATCH=/private/tmp/claude-501/-Users-shaurya-Documents-Dev-processed-future-movement/a9e51102-0a10-4acb-bcd6-470170c27c86/scratchpad
sleep 10
curl -s http://localhost:8082/api/report/csv > "$SCRATCH/compose-output.csv"
diff <(sort "$SCRATCH/compose-output.csv") <(sort sample-output/Output.csv) && echo "CSV MATCHES"
```

Expected: `CSV MATCHES` with no diff output. A `503` here means the state store is not ready yet — wait a few more seconds and retry rather than treating it as a failure.

- [ ] **Step 11: Verify the frontend's nginx proxy works**

This is the step that actually exercises Task 1's rendered upstream; hitting 8082 directly would bypass it entirely.

```bash
curl -s http://localhost:8080/api/report | head -c 120; echo
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/
```

Expected: the first command prints the start of a JSON array containing `Client_Information`; the second prints `200`.

- [ ] **Step 12: Verify the broker-only dev loop still works**

Task 3 documents this command in both service READMEs, so it has to actually work.

```bash
docker compose down -v
docker compose up -d kafka
docker compose ps
```

Expected: exactly one container, `pfm-kafka`. No images build, no other service starts.

```bash
docker compose down -v
```

- [ ] **Step 13: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(compose): run the whole stack with one command

Adds ingestion-service, processing-service, and frontend to docker-compose.yml
alongside a kafka healthcheck, so \`docker compose up -d --build\` brings up the
entire pipeline with no Kubernetes.

processing-service is gated behind a one-shot wait-for-topic service that polls
the broker until future-transactions exists, mirroring the k8s initContainer.
Without it, a cold start kills the StreamThread with a fatal
MissingSourceTopicException that never self-recovers.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: Update the READMEs

`docker compose up -d` no longer means "start a broker". Both service READMEs document it that way and are now wrong.

**Files:**
- Modify: `README.md`
- Modify: `ingestion-service/README.md`
- Modify: `processing-service/README.md`

**Interfaces:**
- Consumes: the commands verified in Task 2 (`docker compose up -d --build`, `docker compose up -d kafka`).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Fix the stale line in the root README**

In `README.md`, replace this sentence under the architecture table:

```
Local dev stack (Kafka, etc.) is defined in `docker-compose.yml` (added alongside the
service implementations).
```

with a full section. Insert it immediately before `## Sample output`:

````markdown
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
so `pfm-wait-for-topic` running briefly is expected, not a failure. Tear down with
`docker compose down -v`.

For the Kubernetes path instead, see [k8s/README.md](k8s/README.md).

### Just the broker

To run the services on the host via Maven and containerize only Kafka — the loop the
service READMEs describe:

```bash
docker compose up -d kafka
```
````

- [ ] **Step 2: Update ingestion-service's dev loop**

In `ingestion-service/README.md`, change the first line of the "Running locally" code block from:

```bash
docker compose up -d                                          # starts a local Kafka broker on localhost:9092
```

to:

```bash
docker compose up -d kafka                                    # starts a local Kafka broker on localhost:9092
```

Leave the rest of that block — the `mvn install` line, the `INGESTION_FILE_PATH` line, and the `-am` explanatory note — exactly as they are.

- [ ] **Step 3: Update processing-service's dev loop**

In `processing-service/README.md`, change:

```bash
docker compose up -d
```

to:

```bash
docker compose up -d kafka
```

- [ ] **Step 4: Verify no stale bare-command references remain**

```bash
grep -rn 'docker compose up' README.md ingestion-service/README.md processing-service/README.md k8s/README.md
```

Expected: every hit is either `docker compose up -d --build` (full stack) or `docker compose up -d kafka` (broker only). A bare `docker compose up -d` with no service and no `--build` means a spot was missed.

- [ ] **Step 5: Commit**

```bash
git add README.md ingestion-service/README.md processing-service/README.md
git commit -m "docs: document the one-command stack, move dev loop to \`up -d kafka\`

\`docker compose up -d\` now starts everything, so the two service READMEs that
documented it as a broker-only step move to \`docker compose up -d kafka\`.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Verify the k8s path still works

Task 1 changed a file the Kubernetes deployment also consumes. Reading the YAML is not evidence — the k8s slice set the precedent of validating against a real cluster, and this is a genuine regression risk.

**Files:** none modified. This task is verification only.

**Interfaces:**
- Consumes: `PROCESSING_SERVICE_UPSTREAM` in `k8s/frontend.yaml` from Task 1.
- Produces: nothing.

- [ ] **Step 1: Free the host ports**

The kind cluster reuses image tags and the port-forwards below collide with Compose.

```bash
docker compose down -v
```

- [ ] **Step 2: Build and load all three images into a fresh cluster**

An unrelated kind cluster named `test` may already exist on this machine — leave it alone; this uses its own `pfm` cluster.

```bash
docker build -f ingestion-service/Dockerfile -t pfm/ingestion-service:local . \
  && docker build -f processing-service/Dockerfile -t pfm/processing-service:local . \
  && docker build -t pfm/frontend:local frontend/
```

```bash
kind create cluster --name pfm \
  && kind load docker-image pfm/ingestion-service:local --name pfm \
  && kind load docker-image pfm/processing-service:local --name pfm \
  && kind load docker-image pfm/frontend:local --name pfm
```

- [ ] **Step 3: Deploy and wait for readiness**

```bash
kubectl apply -f k8s/
kubectl wait --for=condition=ready pod --all -n pfm --timeout=300s
kubectl get pods -n pfm
```

Expected: all four pods `Running`/`Ready` with no restarts. `processing-service` may show `Init:0/1` briefly first — that is its `initContainer`, expected.

- [ ] **Step 4: Confirm the env var actually reached the container**

This is the specific thing Task 1 could have broken.

```bash
kubectl exec -n pfm deploy/frontend -- cat /etc/nginx/conf.d/default.conf | grep upstream
```

Expected: `set $upstream http://processing-service.pfm.svc.cluster.local:8082;` — the value now coming from the manifest rather than the image. If it shows an empty upstream or an unsubstituted `${...}`, the manifest edit is wrong.

- [ ] **Step 5: Verify the report renders end to end through nginx**

```bash
kubectl port-forward -n pfm svc/ingestion-service 18081:8081 &
sleep 3
curl -s -X POST http://localhost:18081/api/ingest | grep -q '"published":717' && echo "ingested"
```

```bash
kubectl port-forward -n pfm svc/frontend 30080:80 &
sleep 3
curl -s http://localhost:30080/api/report | head -c 120; echo
```

Expected: `ingested`, then a JSON array containing `Client_Information` — proving the proxy works with the manifest-supplied upstream.

- [ ] **Step 6: Tear down**

```bash
pkill -f 'kubectl port-forward -n pfm' 2>/dev/null; kind delete cluster --name pfm
```

`pkill` by pattern rather than `kill %1 %2` — job-control job numbers are unreliable in a non-interactive shell, and the pattern is scoped to the `pfm` namespace so it cannot touch port-forwards for the unrelated `test` cluster.

- [ ] **Step 7: Record the result**

No commit if everything passed — this task changes no files. If it surfaced a defect, fix it in `k8s/frontend.yaml` (or `frontend/nginx.conf.template`), re-run Steps 3–5, and commit the fix with a message describing what the cluster run caught.
