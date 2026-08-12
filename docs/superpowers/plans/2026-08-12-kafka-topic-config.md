# Kafka Topic Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the Kafka topic name `future-transactions`, currently declared independently in four places, down to one declaration per environment (Java config, Docker Compose, Kubernetes), so the two services can never disagree on the topic name — whether or not a deploy-time override is set.

**Architecture:** Both Spring Boot services import a new shared classpath resource (`common/src/main/resources/pfm-defaults.yml`) via `spring.config.import` and reference its `pfm.topic` property instead of owning their own literal. This makes drift between the two services structurally impossible, and gives a genuine, framework-guaranteed fail-fast (`ConfigDataResourceNotFoundException`, verified empirically) if that shared resource is ever missing from the packaged jar. Docker Compose gets a single YAML anchor referenced by all three services that need the topic name, for deploy-time overrides. Kubernetes gets a new ConfigMap referenced via `configMapKeyRef` by both deployments and the `wait-for-topic` initContainer, for the same reason, since a YAML anchor can't span the separate manifest files. No `.java` source changes — the duplication was purely in config, and the fix is purely config plus one new YAML resource.

**Tech Stack:** Spring Boot `@ConfigurationProperties`, `spring.config.import`, Docker Compose YAML anchors, Kubernetes ConfigMap.

## Global Constraints

- The topic name itself does not change — it stays `future-transactions` everywhere; only how it's declared changes.
- The literal `future-transactions` default exists in exactly one place: `common/src/main/resources/pfm-defaults.yml`'s `pfm.topic: ${PFM_TOPIC:future-transactions}`. Neither service's own `application.yml` may redeclare it — each references `${pfm.topic}` after importing that file.
- No `.java` source changes anywhere in this plan — only the one new YAML resource file plus edits to existing `application.yml` / Docker Compose / Kubernetes manifest files.

Design doc: [docs/superpowers/specs/2026-08-12-kafka-topic-config-design.md](../specs/2026-08-12-kafka-topic-config-design.md) — see "Revision history" section for why the originally-approved `${PFM_TOPIC}`-with-no-default mechanism was replaced with this one (it turned out not to actually fail Spring context startup).

---

### Task 1: shared topic default in `common` + `ingestion-service` wiring

**Files:**
- Create: `common/src/main/resources/pfm-defaults.yml`
- Modify: `ingestion-service/src/main/resources/application.yml`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: a classpath resource `pfm-defaults.yml` (packaged into `common`'s jar) exposing property `pfm.topic`, resolved as `${PFM_TOPIC:future-transactions}`. Task 2 (`processing-service`) imports this same file — do not create a second copy. `ingestion.topic` now resolves to `${pfm.topic}` via that import.

No test or README changes are expected in this task — see the design doc's "Testing" section for why: the resolved value is identical to what these files already had before this plan started, so nothing observable changes for anything that doesn't explicitly override the topic.

- [ ] **Step 1: Create the shared defaults resource**

`common` currently has no `src/main/resources` directory — create it and
add the file:

Create `common/src/main/resources/pfm-defaults.yml`:

```yaml
pfm:
  topic: ${PFM_TOPIC:future-transactions}
```

- [ ] **Step 2: Build and install `common` so the new resource is on the local classpath**

Run: `mvn -q -pl common -am -DskipTests install`

Expected: BUILD SUCCESS, no output beyond Maven's own warnings. Confirm the
resource made it into the installed jar:

```bash
unzip -l ~/.m2/repository/com/pfm/common/0.1.0-SNAPSHOT/common-0.1.0-SNAPSHOT.jar | grep pfm-defaults.yml
```

Expected: one line showing `pfm-defaults.yml` in the jar listing.

- [ ] **Step 3: Wire `ingestion-service` to import it**

In `ingestion-service/src/main/resources/application.yml`, change:

```yaml
spring:
  application:
    name: ingestion-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

ingestion:
  file-path: ${INGESTION_FILE_PATH:sample-data/Input.txt}
  topic: future-transactions
```

to:

```yaml
spring:
  application:
    name: ingestion-service
  config:
    import: classpath:pfm-defaults.yml
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

ingestion:
  file-path: ${INGESTION_FILE_PATH:sample-data/Input.txt}
  topic: ${pfm.topic}
```

- [ ] **Step 4: Run the test suite and confirm nothing broke**

Run: `mvn -pl ingestion-service -am test`

Expected: PASS — the same tests that passed before this task
(`IngestionServiceApplicationTests`, `KafkaConfigTest`,
`IngestionEndToEndTest`, and the rest) continue to pass unmodified. If any
of them fail, stop: it means `${pfm.topic}` is not resolving the way this
task expects, and that's a real problem to understand before proceeding —
not something to patch over by adding explicit topic properties back into
the tests.

- [ ] **Step 5: Manually confirm the forgotten-env-var case is now harmless**

Run (deliberately omitting `PFM_TOPIC`):

```bash
docker compose up -d kafka
mvn -pl ingestion-service spring-boot:run
```

Expected: starts cleanly (`Started IngestionServiceApplication...`),
`curl -s http://localhost:8081/actuator/health/readiness` reports
`{"status":"UP"}`, and the topic was actually created under the right name:

```bash
docker exec pfm-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected output: `future-transactions` (not a literal `${PFM_TOPIC}` or
`${pfm.topic}` — if you see either of those strings, the import isn't
resolving and something is wrong). Stop the process (`Ctrl+C`) once
confirmed.

- [ ] **Step 6: Manually confirm the missing-resource case genuinely fails fast**

This simulates a packaging defect (the shared resource not making it into
`common`'s jar). Use a real move out of the resources directory, not a
rename within it — `mvn install` does not clean stale files out of
`target/classes`, so renaming in place (e.g. to `.bak`) leaves the old file
on the classpath and silently invalidates this check.

```bash
mv common/src/main/resources/pfm-defaults.yml /tmp/pfm-defaults.yml.stash
mvn -q -pl common clean install -DskipTests
mvn -pl ingestion-service spring-boot:run
```

Expected: `APPLICATION FAILED TO START`, naming
`Config data resource 'class path resource [pfm-defaults.yml]' ... does not exist`,
process exits non-zero. Then restore and rebuild clean before moving on:

```bash
mv /tmp/pfm-defaults.yml.stash common/src/main/resources/pfm-defaults.yml
mvn -q -pl common clean install -DskipTests
docker compose down
```

- [ ] **Step 7: Commit**

```bash
git add common/src/main/resources/pfm-defaults.yml \
        ingestion-service/src/main/resources/application.yml
git commit -m "feat(common,ingestion-service): single-source the Kafka topic default"
```

---

### Task 2: processing-service — wire to the shared topic default

**Files:**
- Modify: `processing-service/src/main/resources/application.yml`

**Interfaces:**
- Consumes: `common/src/main/resources/pfm-defaults.yml` (Task 1) — the file must already exist and be installed to the local `.m2` repo before this task starts (it will be, since Task 1 runs first).
- Produces: `processing.topic` now resolves to `${pfm.topic}` via the same shared import. Nothing else consumes this directly.

No test or README changes are expected in this task, for the same reason as
Task 1.

- [ ] **Step 1: Wire `processing-service` to import the shared defaults**

In `processing-service/src/main/resources/application.yml`, change:

```yaml
spring:
  application:
    name: processing-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    streams:
      application-id: processing-service
      properties:
        processing.guarantee: exactly_once_v2

processing:
  topic: future-transactions
```

to:

```yaml
spring:
  application:
    name: processing-service
  config:
    import: classpath:pfm-defaults.yml
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    streams:
      application-id: processing-service
      properties:
        processing.guarantee: exactly_once_v2

processing:
  topic: ${pfm.topic}
```

- [ ] **Step 2: Run the test suite and confirm nothing broke**

Run: `mvn -pl processing-service -am test`

Expected: PASS — `ProcessingServiceApplicationTests`,
`ProcessingEndToEndTest`, and `FullPipelineGoldenTest` all continue to pass
unmodified. `FullPipelineGoldenTest` in particular sets `processing.topic`
directly as an explicit property and disables default config-file loading,
so it never goes through this import at all — it passing is expected and
not itself proof the import works; the other two tests are what actually
exercise it. If anything fails, stop and understand why before proceeding.

- [ ] **Step 3: Manually confirm the forgotten-env-var case is now harmless**

`processing-service` deliberately never creates the topic itself (see
`k8s/README.md`), so precreate it before starting the service standalone —
starting Kafka Streams against a not-yet-existing source topic is a known
separate flakiness hazard unrelated to this task
(see `ProcessingEndToEndTest`'s `createFutureTransactionsTopic` comment).

```bash
docker compose up -d kafka
docker exec pfm-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic future-transactions --partitions 3 --replication-factor 1
mvn -pl processing-service spring-boot:run
```

Expected (deliberately omitting `PFM_TOPIC`): starts cleanly, and within a
few seconds `curl -s http://localhost:8082/actuator/health/readiness`
reports `{"status":"UP"}` (Kafka Streams reaches `RUNNING`). No
`InvalidTopicException` or `Missing source topics` in the log. Stop the
process (`Ctrl+C`) once confirmed.

- [ ] **Step 4: Manually confirm the missing-resource case still fails fast**

Same check as Task 1 Step 6, run again here because `processing-service` is
a separate module with its own dependency resolution — confirming it here
is not redundant, it's verifying the import actually took effect in this
module too.

```bash
mv common/src/main/resources/pfm-defaults.yml /tmp/pfm-defaults.yml.stash
mvn -q -pl common clean install -DskipTests
mvn -pl processing-service spring-boot:run
```

Expected: `APPLICATION FAILED TO START` naming the missing
`pfm-defaults.yml` config data resource, process exits non-zero. Then
restore:

```bash
mv /tmp/pfm-defaults.yml.stash common/src/main/resources/pfm-defaults.yml
mvn -q -pl common clean install -DskipTests
docker compose down
```

- [ ] **Step 5: Commit**

```bash
git add processing-service/src/main/resources/application.yml
git commit -m "feat(processing-service): single-source the Kafka topic default"
```

---

### Task 3: Docker Compose — single anchor for the topic name

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: both services now resolve their topic via the shared `common/pfm-defaults.yml` import (Tasks 1 and 2), which falls back to `future-transactions` if `PFM_TOPIC` isn't set — so this task's `PFM_TOPIC` values aren't load-bearing for startup, they're for explicitness and future per-deployment overrides. The literal value used here must still match the shared default (`future-transactions`) for that fallback consistency to mean anything.
- Produces: nothing consumed by later tasks (k8s in Task 4 is a separate manifest set).

- [ ] **Step 1: Add the anchor and wire it into all three services**

In `docker-compose.yml`, add the anchor above `services:`:

```yaml
x-topic: &pfm-topic future-transactions

services:
```

Add `PFM_TOPIC` to `ingestion-service`'s environment block:

```yaml
    environment:
      KAFKA_BOOTSTRAP_SERVERS: 'broker:19092'
      PFM_TOPIC: *pfm-topic
```

Add an `environment` block to `wait-for-topic` and change its command to
read `$PFM_TOPIC` from its own container environment instead of the
literal. Note the `$$` — Compose interpolates `$VAR`/`${VAR}` in the file
itself, so `$$` is required to pass a literal `$` through to the container's
shell:

```yaml
  wait-for-topic:
    image: apache/kafka:3.9.2
    container_name: pfm-wait-for-topic
    environment:
      PFM_TOPIC: *pfm-topic
    depends_on:
      kafka:
        condition: service_healthy
    command:
      - sh
      - -c
      - |
        until /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:19092 --list | grep -qx "$$PFM_TOPIC"; do
          echo "waiting for $$PFM_TOPIC topic..."
          sleep 2
        done
        echo "$$PFM_TOPIC topic found, starting processing-service"
```

Add `PFM_TOPIC` to `processing-service`'s environment block:

```yaml
    environment:
      KAFKA_BOOTSTRAP_SERVERS: 'broker:19092'
      PFM_TOPIC: *pfm-topic
```

- [ ] **Step 2: Statically verify the anchor resolves and escaping is correct**

Run: `docker compose config`

Expected: the rendered output shows `PFM_TOPIC: future-transactions` under
all three services (`ingestion-service`, `wait-for-topic`,
`processing-service`), and the `wait-for-topic` command block shows the
literal string `$PFM_TOPIC` (single `$`, un-interpolated) inside the shell
script — proving the `$$` escaping worked and Compose did not try to
resolve it as its own variable (which would render as empty).

- [ ] **Step 3: End-to-end smoke test**

Run:

```bash
docker compose down -v
docker compose up -d --build
```

Expected: `wait-for-topic` logs the repeating "waiting for
future-transactions topic..." line until `ingestion-service` comes up and
creates the topic, then exits 0 with "future-transactions topic found,
starting processing-service"; `processing-service` then starts. Confirm
with:

```bash
docker compose logs wait-for-topic
docker compose ps
```

Then tear down:

```bash
docker compose down
```

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "feat(docker-compose): single-source the Kafka topic name via a YAML anchor"
```

---

### Task 4: Kubernetes — ConfigMap for the topic name

**Files:**
- Create: `k8s/01-topic-config.yaml`
- Modify: `k8s/ingestion-service.yaml`
- Modify: `k8s/processing-service.yaml`
- Modify: `k8s/README.md`

**Interfaces:**
- Consumes: both services now resolve their topic via the shared
  `common/pfm-defaults.yml` import (Tasks 1 and 2), which falls back to
  `future-transactions` if `PFM_TOPIC` isn't set — so, as with Task 3, this
  ConfigMap's value isn't load-bearing for startup, it's for explicitness
  and future per-deployment overrides. It must still match the shared
  default (`future-transactions`).
- Produces: nothing consumed by later tasks (this is the last task).

Note on the filename: the existing `k8s/00-namespace.yaml` uses a numeric
prefix specifically to control apply order (the namespace must exist before
anything else). `kubectl apply -f k8s/` applies files in filename order, and
without a prefix `topic-config.yaml` would sort after `processing-service.yaml`
(t > p) — meaning the Deployment referencing the ConfigMap would be applied
before the ConfigMap exists. `01-topic-config.yaml` sorts right after the
namespace and before every Deployment, following the same convention.

- [ ] **Step 1: Create the ConfigMap**

Create `k8s/01-topic-config.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: pfm-topic-config
  namespace: pfm
data:
  PFM_TOPIC: future-transactions
```

- [ ] **Step 2: Wire it into `ingestion-service.yaml`**

In `k8s/ingestion-service.yaml`, change:

```yaml
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka:9092"
```

to:

```yaml
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka:9092"
            - name: PFM_TOPIC
              valueFrom:
                configMapKeyRef:
                  name: pfm-topic-config
                  key: PFM_TOPIC
```

- [ ] **Step 3: Wire it into `processing-service.yaml`'s initContainer and main container**

In `k8s/processing-service.yaml`, change the initContainer from:

```yaml
      initContainers:
        - name: wait-for-topic
          image: apache/kafka:3.9.2
          command:
            - sh
            - -c
            - |
              until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -qx future-transactions; do
                echo "waiting for future-transactions topic..."
                sleep 2
              done
              echo "future-transactions topic found, starting processing-service"
```

to:

```yaml
      initContainers:
        - name: wait-for-topic
          image: apache/kafka:3.9.2
          env:
            - name: PFM_TOPIC
              valueFrom:
                configMapKeyRef:
                  name: pfm-topic-config
                  key: PFM_TOPIC
          command:
            - sh
            - -c
            - |
              until /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -qx "$PFM_TOPIC"; do
                echo "waiting for $PFM_TOPIC topic..."
                sleep 2
              done
              echo "$PFM_TOPIC topic found, starting processing-service"
```

Note: unlike Compose, plain `kubectl apply` does no variable interpolation
of its own on the manifest, so `$PFM_TOPIC` here needs no `$$` escaping —
it reaches the container's `sh -c` literally and is expanded from that
container's own environment at runtime.

Then change the main container's env block from:

```yaml
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka:9092"
```

to:

```yaml
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka:9092"
            - name: PFM_TOPIC
              valueFrom:
                configMapKeyRef:
                  name: pfm-topic-config
                  key: PFM_TOPIC
```

- [ ] **Step 4: Note the ConfigMap in `k8s/README.md`**

In `k8s/README.md`, after the paragraph ending "...versus signs Kafka never
came up." (currently lines 30-35), add:

```markdown
The topic name is defined once, in `k8s/01-topic-config.yaml`'s
`pfm-topic-config` ConfigMap, and referenced by `ingestion-service`,
`processing-service`, and this init container via a `PFM_TOPIC` env var —
not hardcoded per-manifest.
```

- [ ] **Step 5: Verify on a fresh kind cluster**

Run:

```bash
docker build -f ingestion-service/Dockerfile -t pfm/ingestion-service:local .
docker build -f processing-service/Dockerfile -t pfm/processing-service:local .
docker build -t pfm/frontend:local frontend/

kind create cluster --name pfm
kind load docker-image pfm/ingestion-service:local --name pfm
kind load docker-image pfm/processing-service:local --name pfm
kind load docker-image pfm/frontend:local --name pfm

kubectl apply -f k8s/
```

Expected: `processing-service`'s pod shows `Init:0/1` briefly (same as
before this change), then reaches `Running`/`1/1` once `ingestion-service`
creates the topic. Confirm the ConfigMap is actually what's driving it:

```bash
kubectl get configmap -n pfm pfm-topic-config -o yaml
kubectl exec -n pfm deploy/ingestion-service -- env | grep PFM_TOPIC
kubectl logs -n pfm deploy/processing-service -c wait-for-topic
```

Expected: the ConfigMap shows `PFM_TOPIC: future-transactions`, the
`ingestion-service` container's env includes the same, and the
`wait-for-topic` log lines mention `future-transactions` (sourced from
`$PFM_TOPIC`, not a literal in the manifest).

Tear down:

```bash
kind delete cluster --name pfm
```

- [ ] **Step 6: Commit**

```bash
git add k8s/01-topic-config.yaml k8s/ingestion-service.yaml k8s/processing-service.yaml k8s/README.md
git commit -m "feat(k8s): single-source the Kafka topic name via a ConfigMap"
```
