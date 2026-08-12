# API Versioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prefix all three app-level REST endpoints (`POST /api/ingest`, `GET /api/report`, `GET /api/report/csv`) with `/v1`, and update every test, frontend call, and doc that references the old paths.

**Architecture:** Pure URI-path rename. `@RequestMapping`/`@GetMapping` annotations on the two Spring controllers gain a `/v1` segment; every consumer (Angular frontend, Spring MockMvc/Testcontainers tests, curl examples in READMEs) is updated to match. No new abstractions, no shared version constant (see spec for why), no behavior change.

**Tech Stack:** Java 21 / Spring Boot (ingestion-service, processing-service), Angular 21 / vitest (frontend), Maven multi-module build, Testcontainers (Kafka) for backend E2E tests.

## Global Constraints

- Endpoint paths become `POST /api/v1/ingest`, `GET /api/v1/report`, `GET /api/v1/report/csv`. Exactly `/v1` — no other segment naming.
- Actuator endpoints (`/actuator/health/*`) are NOT versioned and are out of scope.
- `frontend/nginx.conf.template` and `frontend/proxy.conf.json` are NOT modified — both already proxy the whole `/api/` prefix as a passthrough (see spec's "Out of scope" section).
- `docker-compose.yml`, `k8s/*.yaml`, and existing files under `docs/superpowers/plans/*.md` / `docs/superpowers/specs/*.md` are NOT modified — no API path is baked into any manifest, and existing dated design docs are historical records, not living docs.
- Design reference: `docs/superpowers/specs/2026-08-12-api-versioning-design.md`.

---

### Task 1: ingestion-service — version `POST /api/ingest`

**Files:**
- Modify: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java:11`
- Modify: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionControllerTest.java` (lines 34, 50, 60, 69)
- Modify: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java:139`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `IngestionController` now serves `POST /api/v1/ingest` (and `POST /api/v1/ingest?force=true`). Task 3 (frontend) doesn't call this endpoint directly — only Task 2's `FullPipelineGoldenTest` and Task 4 (docs) depend on this new path.

- [ ] **Step 1: Update the MockMvc test to expect the new path**

In `ingestion-service/src/test/java/com/pfm/ingestion/IngestionControllerTest.java`, change all four `mockMvc.perform(post("/api/ingest")...)` call sites to `post("/api/v1/ingest")`:
- Line 34: `mockMvc.perform(post("/api/ingest"))` → `mockMvc.perform(post("/api/v1/ingest"))`
- Line 50: `mockMvc.perform(post("/api/ingest").param("force", "true"))` → `mockMvc.perform(post("/api/v1/ingest").param("force", "true"))`
- Line 60: `mockMvc.perform(post("/api/ingest"))` → `mockMvc.perform(post("/api/v1/ingest"))`
- Line 69: `mockMvc.perform(post("/api/ingest"))` → `mockMvc.perform(post("/api/v1/ingest"))`

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl ingestion-service -am test -Dtest=IngestionControllerTest`
Expected: FAIL — all four tests get 404 instead of the expected status, because `IngestionController` still maps `/api/ingest`, not `/api/v1/ingest`.

- [ ] **Step 3: Update the controller mapping**

In `ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java:11`, change:

```java
@RequestMapping("/api")
```

to:

```java
@RequestMapping("/api/v1")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl ingestion-service -am test -Dtest=IngestionControllerTest`
Expected: PASS (all 4 tests).

- [ ] **Step 5: Update the Testcontainers end-to-end test**

In `ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java:139`, change:

```java
restTemplate.postForEntity("/api/ingest" + querySuffix, null, IngestionResult.class);
```

to:

```java
restTemplate.postForEntity("/api/v1/ingest" + querySuffix, null, IngestionResult.class);
```

- [ ] **Step 6: Run the end-to-end test to verify it passes**

Requires Docker running locally (Testcontainers spins up a real Kafka broker).

Run: `mvn -pl ingestion-service -am test -Dtest=IngestionEndToEndTest`
Expected: PASS. This test also validates the wire contract (Kafka payload/headers) is unaffected by the path rename — confirming the rename touched routing only, not behavior.

- [ ] **Step 7: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionControllerTest.java \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java
git commit -m "feat(ingestion-service): version POST /api/ingest as /api/v1/ingest"
```

---

### Task 2: processing-service — version `GET /api/report` and `GET /api/report/csv`

**Files:**
- Modify: `processing-service/src/main/java/com/pfm/processing/report/ReportController.java` (lines 23, 28)
- Modify: `processing-service/src/test/java/com/pfm/processing/report/ReportControllerTest.java` (lines 32, 45, 66, 76)
- Modify: `processing-service/src/test/java/com/pfm/processing/ProcessingEndToEndTest.java:126`
- Modify: `processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java` (lines 86, 106)

**Interfaces:**
- Consumes: Task 1's `IngestionController` now serving `POST /api/v1/ingest` — `FullPipelineGoldenTest` in this task calls that endpoint directly by URL string, so Task 1 must be committed first.
- Produces: `ReportController` now serves `GET /api/v1/report` and `GET /api/v1/report/csv`. Task 3 (frontend) and Task 4 (docs) depend on this new path.

- [ ] **Step 1: Update the MockMvc test to expect the new path**

In `processing-service/src/test/java/com/pfm/processing/report/ReportControllerTest.java`:
- Line 32: `mockMvc.perform(get("/api/report"))` → `mockMvc.perform(get("/api/v1/report"))`
- Line 45: `mockMvc.perform(get("/api/report"))` → `mockMvc.perform(get("/api/v1/report"))`
- Line 66: `mockMvc.perform(get("/api/report/csv"))` → `mockMvc.perform(get("/api/v1/report/csv"))`
- Line 76: `mockMvc.perform(get("/api/report"))` → `mockMvc.perform(get("/api/v1/report"))`

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl processing-service -am test -Dtest=ReportControllerTest`
Expected: FAIL — all four tests get 404 instead of the expected status, because `ReportController` still maps `/api/report` and `/api/report/csv`.

- [ ] **Step 3: Update the controller mappings**

In `processing-service/src/main/java/com/pfm/processing/report/ReportController.java`:

Line 23, change:
```java
@GetMapping("/api/report")
```
to:
```java
@GetMapping("/api/v1/report")
```

Line 28, change:
```java
@GetMapping(value = "/api/report/csv", produces = "text/csv")
```
to:
```java
@GetMapping(value = "/api/v1/report/csv", produces = "text/csv")
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl processing-service -am test -Dtest=ReportControllerTest`
Expected: PASS (all 4 tests).

- [ ] **Step 5: Update the Kafka Streams end-to-end test**

In `processing-service/src/test/java/com/pfm/processing/ProcessingEndToEndTest.java:126`, change:

```java
ResponseEntity<ReportEntry[]> response = restTemplate.getForEntity("/api/report", ReportEntry[].class);
```

to:

```java
ResponseEntity<ReportEntry[]> response = restTemplate.getForEntity("/api/v1/report", ReportEntry[].class);
```

- [ ] **Step 6: Update the full-pipeline golden test**

In `processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java`:

Line 86, change:
```java
IngestionResult ingestResult = rest.postForObject(
        "http://localhost:18081/api/ingest", null, IngestionResult.class);
```
to:
```java
IngestionResult ingestResult = rest.postForObject(
        "http://localhost:18081/api/v1/ingest", null, IngestionResult.class);
```

Line 106, change:
```java
lastBody = rest.getForObject("http://localhost:18082/api/report/csv", String.class);
```
to:
```java
lastBody = rest.getForObject("http://localhost:18082/api/v1/report/csv", String.class);
```

- [ ] **Step 7: Run both end-to-end tests to verify they pass**

Requires Docker running locally (Testcontainers spins up a real Kafka broker). `FullPipelineGoldenTest` boots both a real `IngestionServiceApplication` and `ProcessingServiceApplication` in-process, so it exercises Task 1's change too — confirming the two services agree on the new paths end to end.

Run: `mvn -pl processing-service -am test -Dtest=ProcessingEndToEndTest,FullPipelineGoldenTest`
Expected: PASS (both tests). `FullPipelineGoldenTest` asserts 717 published records and an exact CSV match against `sample-output/Output.csv` — unchanged by the rename, confirming behavior didn't shift.

- [ ] **Step 8: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/report/ReportController.java \
        processing-service/src/test/java/com/pfm/processing/report/ReportControllerTest.java \
        processing-service/src/test/java/com/pfm/processing/ProcessingEndToEndTest.java \
        processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java
git commit -m "feat(processing-service): version GET /api/report and /api/report/csv as /api/v1/*"
```

---

### Task 3: frontend — call the versioned report endpoints

**Files:**
- Modify: `frontend/src/app/report/report.service.ts:30`
- Modify: `frontend/src/app/report/report.html:46`
- Modify: `frontend/src/app/report/report.service.spec.ts` (13 occurrences of `/api/report`)
- Modify: `frontend/src/app/report/report.integration.spec.ts` (lines 35, 46, 61)
- Modify: `frontend/src/app/report/report.spec.ts` (line 98)

**Interfaces:**
- Consumes: Task 2's `ReportController` now serving `GET /api/v1/report` and `GET /api/v1/report/csv`.
- Produces: nothing consumed by later tasks — this is the last code task.

- [ ] **Step 1: Update the spec files to expect the new paths**

In `frontend/src/app/report/report.service.spec.ts`, replace every `httpMock.expectOne('/api/report')` with `httpMock.expectOne('/api/v1/report')`, and the `httpMock.expectNone('/api/report')` at line 146 with `httpMock.expectNone('/api/v1/report')`. There are 13 occurrences total (lines 39, 48, 56, 67, 77, 84, 90, 96, 105, 112, 119, 129, 137, 146, 151, 156 — every `/api/report` string literal in the file).

In `frontend/src/app/report/report.integration.spec.ts`:
- Line 35: `httpMock.expectOne('/api/report')` → `httpMock.expectOne('/api/v1/report')`
- Line 46: `httpMock.expectOne('/api/report')` → `httpMock.expectOne('/api/v1/report')`
- Line 61: `expect(csvLink.getAttribute('href')).toBe('/api/report/csv');` → `expect(csvLink.getAttribute('href')).toBe('/api/v1/report/csv');`

In `frontend/src/app/report/report.spec.ts:98`:
```typescript
expect(csvLink.getAttribute('href')).toBe('/api/report/csv');
```
becomes:
```typescript
expect(csvLink.getAttribute('href')).toBe('/api/v1/report/csv');
```

- [ ] **Step 2: Run the frontend tests to verify they fail**

Run (from `frontend/`): `npm test`
Expected: FAIL — `report.service.spec.ts` and `report.integration.spec.ts` tests fail because `httpMock.expectOne('/api/v1/report')` finds no matching request (the service still calls `/api/report`); `report.spec.ts`'s CSV-link assertion fails because `report.html` still renders the old `href`.

- [ ] **Step 3: Update the service call and the CSV link**

In `frontend/src/app/report/report.service.ts:30`, change:

```typescript
this.http.get<ReportEntry[]>('/api/report').subscribe({
```

to:

```typescript
this.http.get<ReportEntry[]>('/api/v1/report').subscribe({
```

In `frontend/src/app/report/report.html:46`, change:

```html
<a data-testid="csv-download" href="/api/report/csv" download>Download CSV</a>
```

to:

```html
<a data-testid="csv-download" href="/api/v1/report/csv" download>Download CSV</a>
```

- [ ] **Step 4: Run the frontend tests to verify they pass**

Run (from `frontend/`): `npm test`
Expected: PASS (all suites).

- [ ] **Step 5: Commit**

```bash
git add frontend/src/app/report/report.service.ts \
        frontend/src/app/report/report.html \
        frontend/src/app/report/report.service.spec.ts \
        frontend/src/app/report/report.integration.spec.ts \
        frontend/src/app/report/report.spec.ts
git commit -m "feat(frontend): call /api/v1/report and /api/v1/report/csv"
```

---

### Task 4: update README usage docs

**Files:**
- Modify: `README.md` (line 46 curl; lines 77, 83 prose endpoint mentions)
- Modify: `ingestion-service/README.md` (lines 36, 41, 45 — curl and prose)
- Modify: `processing-service/README.md` (lines 8, 9, 27, 28 — prose and curl)
- Modify: `k8s/README.md` (lines 41, 44, 53 — prose and curl)

These are living usage docs (not the dated `docs/superpowers/plans|specs` historical records — see Global Constraints), so every literal endpoint path mention is updated, not just the curl commands, to avoid leaving stale documentation next to correct examples.

**Interfaces:**
- Consumes: Task 1 and Task 2's new paths (documents them; no code dependency).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Update root `README.md`**

Line 46:
```bash
curl -X POST http://localhost:8081/api/ingest
```
becomes:
```bash
curl -X POST http://localhost:8081/api/v1/ingest
```

Line 77 prose:
```
- `ingestion-service` — done: `POST /api/ingest` reads the file and publishes to Kafka
```
becomes:
```
- `ingestion-service` — done: `POST /api/v1/ingest` reads the file and publishes to Kafka
```

Line 83 prose:
```
  `GET /api/report` and `GET /api/report/csv`. See its
```
becomes:
```
  `GET /api/v1/report` and `GET /api/v1/report/csv`. See its
```

- [ ] **Step 2: Update `ingestion-service/README.md`**

Line 36:
```bash
curl -X POST http://localhost:8081/api/ingest
```
becomes:
```bash
curl -X POST http://localhost:8081/api/v1/ingest
```

Line 41:
```
- `POST /api/ingest` — parses the configured file and publishes each record to Kafka.
```
becomes:
```
- `POST /api/v1/ingest` — parses the configured file and publishes each record to Kafka.
```

Line 45:
```
- `POST /api/ingest?force=true` — bypasses the cache and republishes even if this exact
```
becomes:
```
- `POST /api/v1/ingest?force=true` — bypasses the cache and republishes even if this exact
```

- [ ] **Step 3: Update `processing-service/README.md`**

Line 8:
```
- `GET /api/report` — JSON
```
becomes:
```
- `GET /api/v1/report` — JSON
```

Line 9:
```
- `GET /api/report/csv` — CSV download (`Output.csv` format)
```
becomes:
```
- `GET /api/v1/report/csv` — CSV download (`Output.csv` format)
```

Line 27:
```bash
curl http://localhost:8082/api/report
```
becomes:
```bash
curl http://localhost:8082/api/v1/report
```

Line 28:
```bash
curl http://localhost:8082/api/report/csv
```
becomes:
```bash
curl http://localhost:8082/api/v1/report/csv
```

- [ ] **Step 4: Update `k8s/README.md`**

Line 41 prose:
```
so a `POST /api/ingest` that lands before Kafka's listener is accepting connections
```
becomes:
```
so a `POST /api/v1/ingest` that lands before Kafka's listener is accepting connections
```

Line 44 prose:
```
`POST /api/ingest` within ~1s of `ingestion-service` coming up (once via the `Service`,
```
becomes:
```
`POST /api/v1/ingest` within ~1s of `ingestion-service` coming up (once via the `Service`,
```

Line 53:
```bash
curl -X POST http://localhost:18081/api/ingest
```
becomes:
```bash
curl -X POST http://localhost:18081/api/v1/ingest
```

- [ ] **Step 5: Verify no stale references remain**

Run: `grep -rn "api/ingest\b\|api/report\b\|api/report/csv" README.md ingestion-service/README.md processing-service/README.md k8s/README.md`
Expected: every match shown includes `/v1/`. (The `\b` after `ingest`/`report` prevents this from also matching the now-correct `/v1/` paths, so any bare hit here is a miss.)

- [ ] **Step 6: Commit**

```bash
git add README.md ingestion-service/README.md processing-service/README.md k8s/README.md
git commit -m "docs: update curl examples and endpoint references to /api/v1"
```

---

### Task 5: full-stack verification through docker-compose

**Files:** none modified — this task only runs and observes the already-committed changes.

**Interfaces:**
- Consumes: Tasks 1–4, fully committed.
- Produces: confirmation the rename works end to end, including through nginx (the one piece of infrastructure that needed zero code change and is therefore the easiest to under-test — see the spec's manual-verification note).

- [ ] **Step 1: Run the full backend build**

Run (from repo root): `mvn -B clean verify -Dtestcontainers.docker.api.version=`
Expected: BUILD SUCCESS across `common`, `ingestion-service`, `processing-service`.

- [ ] **Step 2: Run the full frontend build**

Run (from `frontend/`): `npm test && npm run build`
Expected: all tests pass, build succeeds.

- [ ] **Step 3: Bring up the full stack**

Run (from repo root): `docker compose up -d --build`
Expected: `pfm-kafka`, `ingestion-service`, `processing-service`, `frontend` all come up; `pfm-wait-for-topic` shows `Exited (0)` in `docker compose ps -a`.

- [ ] **Step 4: Verify the versioned endpoints directly against each backend service**

```bash
curl -X POST http://localhost:8081/api/v1/ingest
curl http://localhost:8082/api/v1/report
curl http://localhost:8082/api/v1/report/csv
```
Expected: ingest reports `"published":717`; report/csv return the aggregated data (CSV matches `sample-output/Output.csv`).

- [ ] **Step 5: Verify the versioned report endpoint through the nginx-proxied frontend**

This is the step that specifically catches an nginx misconfiguration that direct-to-backend curl (Step 4) cannot — see the spec's manual-verification note.

```bash
curl -s http://localhost:8080/api/v1/report | head -c 200; echo
```
Expected: same JSON report data as the direct `:8082` call in Step 4 — proving nginx's `proxy_pass $upstream;` passthrough forwards the `/v1` segment unmodified. Also open `http://localhost:8080` in a browser and confirm the table renders and "Download CSV" works (exercises `report.service.ts` and the `report.html` link end to end).

- [ ] **Step 6: Tear down**

```bash
docker compose down -v
```

- [ ] **Step 7: No commit for this task** (verification only — nothing to stage). If any step failed, fix the issue in the relevant earlier task, re-commit there, and re-run this task from Step 1.
