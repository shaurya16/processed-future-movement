# Kafka Topic Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collapse the Kafka topic name `future-transactions`, currently declared independently in four places, down to one declaration per environment (app config, Docker Compose, Kubernetes), so a forgotten update fails loudly at startup instead of silently misconfiguring the pipeline.

**Architecture:** Both Spring Boot services switch from a hardcoded `application.yml` literal to a required `${PFM_TOPIC}` placeholder with no default (fails fast if unset). Docker Compose gets a single YAML anchor referenced by all three services that need the topic name. Kubernetes gets a new ConfigMap referenced via `configMapKeyRef` by both deployments and the `wait-for-topic` initContainer, since a YAML anchor can't span the separate manifest files. No production Java code changes — the duplication was purely in config.

**Tech Stack:** Spring Boot `@ConfigurationProperties`, Docker Compose YAML anchors, Kubernetes ConfigMap.

## Global Constraints

- The topic name itself does not change — it stays `future-transactions` everywhere; only how it's declared changes.
- No default value for `ingestion.topic` / `processing.topic` in `application.yml` — an unset `PFM_TOPIC` must fail Spring context startup, not silently fall back.
- No change to `src/main` Java code in either service (`IngestionProperties`, `ProcessingProperties` already read the property correctly).

Design doc: [docs/superpowers/specs/2026-08-12-kafka-topic-config-design.md](../specs/2026-08-12-kafka-topic-config-design.md)

---

### Task 1: ingestion-service — required `PFM_TOPIC` property

**Files:**
- Modify: `ingestion-service/src/main/resources/application.yml:12`
- Modify: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceApplicationTests.java:12-14`
- Modify: `ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaConfigTest.java:13`
- Modify: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java` (the `@DynamicPropertySource` method)
- Modify: `ingestion-service/README.md:19`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `ingestion.topic` now resolves from the `PFM_TOPIC` env var with no fallback. Task 3 (Compose) and Task 4 (k8s) both rely on this — they must supply `PFM_TOPIC` in the container environment or `ingestion-service` will fail to start.

- [ ] **Step 1: Remove the hardcoded default in `application.yml`**

In `ingestion-service/src/main/resources/application.yml`, change:

```yaml
ingestion:
  file-path: ${INGESTION_FILE_PATH:sample-data/Input.txt}
  topic: future-transactions
```

to:

```yaml
ingestion:
  file-path: ${INGESTION_FILE_PATH:sample-data/Input.txt}
  topic: ${PFM_TOPIC}
```

- [ ] **Step 2: Run the test suite and confirm it fails for the right reason**

Run: `mvn -pl ingestion-service -am test`

Expected: FAIL. `IngestionServiceApplicationTests`, `KafkaConfigTest`, and
`IngestionEndToEndTest` should each fail during Spring context startup with
an error resolving the placeholder `'PFM_TOPIC'` (e.g.
`IllegalArgumentException: Could not resolve placeholder 'PFM_TOPIC'`). This
confirms the property is now genuinely required, and identifies exactly the
three tests that were silently relying on the old default.

- [ ] **Step 3: Make each affected test set `ingestion.topic` explicitly**

In `IngestionServiceApplicationTests.java`, change:

```java
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.admin.auto-create=false")
```

to:

```java
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.kafka.admin.auto-create=false", "ingestion.topic=future-transactions"})
```

In `KafkaConfigTest.java`, change:

```java
@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
```

to:

```java
@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ingestion.topic=future-transactions"})
```

In `IngestionEndToEndTest.java`, change:

```java
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("ingestion.file-path", IngestionEndToEndTest::sampleFilePath);
    }
```

to:

```java
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("ingestion.file-path", IngestionEndToEndTest::sampleFilePath);
        registry.add("ingestion.topic", () -> "future-transactions");
    }
```

- [ ] **Step 4: Run the test suite again and confirm it passes**

Run: `mvn -pl ingestion-service -am test`

Expected: PASS, all tests including `IngestionServiceApplicationTests`,
`KafkaConfigTest`, and `IngestionEndToEndTest`.

- [ ] **Step 5: Update the README's standalone run command**

In `ingestion-service/README.md`, change:

```bash
INGESTION_FILE_PATH="$PWD/sample-data/Input.txt" mvn -pl ingestion-service spring-boot:run
```

to:

```bash
PFM_TOPIC=future-transactions INGESTION_FILE_PATH="$PWD/sample-data/Input.txt" mvn -pl ingestion-service spring-boot:run
```

- [ ] **Step 6: Manually confirm the fail-fast behavior**

Run (deliberately omitting `PFM_TOPIC`):

```bash
docker compose up -d kafka
mvn -q -DskipTests install
mvn -pl ingestion-service spring-boot:run
```

Expected: the app fails to start with a clear Spring error naming the
unresolved `PFM_TOPIC` placeholder — not a silent empty/default topic.
Then confirm the documented command from Step 5 (with `PFM_TOPIC` set)
starts cleanly. Stop the process (`Ctrl+C`) once confirmed;
`docker compose down` afterward if nothing else in this session needs Kafka
running.

- [ ] **Step 7: Commit**

```bash
git add ingestion-service/src/main/resources/application.yml \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceApplicationTests.java \
        ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaConfigTest.java \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java \
        ingestion-service/README.md
git commit -m "feat(ingestion-service): require PFM_TOPIC, drop hardcoded topic default"
```

---

### Task 2: processing-service — required `PFM_TOPIC` property

**Files:**
- Modify: `processing-service/src/main/resources/application.yml:15`
- Modify: `processing-service/src/test/java/com/pfm/processing/ProcessingServiceApplicationTests.java:12-14`
- Modify: `processing-service/src/test/java/com/pfm/processing/ProcessingEndToEndTest.java` (the `@DynamicPropertySource` method)
- Modify: `processing-service/README.md:20`

**Interfaces:**
- Consumes: nothing from other tasks (independent of Task 1 — different module, different property prefix).
- Produces: `processing.topic` now resolves from the `PFM_TOPIC` env var with no fallback. Task 3 and Task 4 both rely on this.

- [ ] **Step 1: Remove the hardcoded default in `application.yml`**

In `processing-service/src/main/resources/application.yml`, change:

```yaml
processing:
  topic: future-transactions
```

to:

```yaml
processing:
  topic: ${PFM_TOPIC}
```

- [ ] **Step 2: Run the test suite and confirm it fails for the right reason**

Run: `mvn -pl processing-service -am test`

Expected: FAIL. `ProcessingServiceApplicationTests` and
`ProcessingEndToEndTest` should each fail during Spring context startup
with an error resolving the placeholder `'PFM_TOPIC'`.
`FullPipelineGoldenTest` should still PASS unaffected — it already sets
`processing.topic=future-transactions` (and `ingestion.topic=future-transactions`)
as explicit properties and disables default config-file loading, so it
never depended on the `application.yml` default in the first place. If it
fails too, stop and re-examine — that would mean this change has a wider
blast radius than expected.

- [ ] **Step 3: Make each affected test set `processing.topic` explicitly**

In `ProcessingServiceApplicationTests.java`, change:

```java
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.streams.auto-startup=false")
```

to:

```java
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"spring.kafka.streams.auto-startup=false", "processing.topic=future-transactions"})
```

In `ProcessingEndToEndTest.java`, change:

```java
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.streams.application-id", () -> "processing-service-e2e-test");
    }
```

to:

```java
    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.streams.application-id", () -> "processing-service-e2e-test");
        registry.add("processing.topic", () -> "future-transactions");
    }
```

- [ ] **Step 4: Run the test suite again and confirm it passes**

Run: `mvn -pl processing-service -am test`

Expected: PASS, all tests including `ProcessingServiceApplicationTests`,
`ProcessingEndToEndTest`, and `FullPipelineGoldenTest`.

- [ ] **Step 5: Update the README's standalone run command**

In `processing-service/README.md`, change:

```bash
mvn -pl processing-service spring-boot:run
```

to:

```bash
PFM_TOPIC=future-transactions mvn -pl processing-service spring-boot:run
```

- [ ] **Step 6: Commit**

```bash
git add processing-service/src/main/resources/application.yml \
        processing-service/src/test/java/com/pfm/processing/ProcessingServiceApplicationTests.java \
        processing-service/src/test/java/com/pfm/processing/ProcessingEndToEndTest.java \
        processing-service/README.md
git commit -m "feat(processing-service): require PFM_TOPIC, drop hardcoded topic default"
```

---

### Task 3: Docker Compose — single anchor for the topic name

**Files:**
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: `ingestion-service` and `processing-service` now require `PFM_TOPIC` in their container environment (Tasks 1 and 2) or they fail to start.
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
- Consumes: `ingestion-service` and `processing-service` now require
  `PFM_TOPIC` in their container environment (Tasks 1 and 2) or they fail to
  start.
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
