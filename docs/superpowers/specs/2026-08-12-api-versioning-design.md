# API Versioning — Design

**Date:** 2026-08-12
**Branch:** feature/api-versioning (off main @ 383567e)

## Problem

The three REST endpoints in this system (`POST /api/ingest`, `GET /api/report`,
`GET /api/report/csv`) carry no version marker. Introducing one now, while there's
still only one client (the Angular frontend, plus manual curl/demo usage), avoids a
breaking migration later if the API needs to evolve.

## Decision

URI-path versioning: prefix every app-level endpoint with `/v1`.

| Before | After | Service |
|---|---|---|
| `POST /api/ingest` | `POST /api/v1/ingest` | ingestion-service |
| `GET /api/report` | `GET /api/v1/report` | processing-service |
| `GET /api/report/csv` | `GET /api/v1/report/csv` | processing-service |

Rejected alternative: header-based versioning (`Accept: application/vnd.pfm.v1+json`).
Adds content-negotiation complexity with no present payoff for 3 endpoints and 1
client — YAGNI.

Rejected alternative: a shared `ApiVersion` constant. With 2 controllers and 3
mappings, hardcoding the literal `/api/v1` in each `@RequestMapping`/`@GetMapping` is
simpler than introducing an abstraction for a single version number.

Actuator endpoints (`/actuator/health/*`) are infrastructure, not app API, and are
out of scope.

## Components affected

**Backend**
- `ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java` —
  `@RequestMapping("/api")` → `@RequestMapping("/api/v1")`
- `processing-service/src/main/java/com/pfm/processing/report/ReportController.java` —
  both `@GetMapping` paths gain `/v1`

**Backend tests** (assert the literal path, must move with it)
- `ingestion-service/src/test/java/com/pfm/ingestion/IngestionControllerTest.java`
- `ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java`
- `processing-service/src/test/java/com/pfm/processing/report/ReportControllerTest.java`
- `processing-service/src/test/java/com/pfm/processing/ProcessingEndToEndTest.java`
- `processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java`

**Frontend**
- `frontend/src/app/report/report.service.ts` — fetch URL
- `frontend/src/app/report/report.html` — hardcoded CSV download `href`
- `frontend/src/app/report/report.service.spec.ts`
- `frontend/src/app/report/report.integration.spec.ts`
- `frontend/src/app/report/report.spec.ts`

**Docs** (living usage docs, updated in place)
- `README.md`
- `ingestion-service/README.md`
- `processing-service/README.md`
- `k8s/README.md`

## Out of scope / unaffected

- `frontend/nginx.conf.template`, `frontend/proxy.conf.json` — both proxy the entire
  `/api/` prefix as a passthrough; `/api/v1/...` requires no config change.
- `docker-compose.yml`, `k8s/*.yaml` — no API path baked into any manifest.
- `docs/superpowers/plans/*.md`, `docs/superpowers/specs/*.md` (existing files) —
  these are dated historical records of already-completed slices. They describe the
  API as it existed at the time and are not updated retroactively.

## Error handling

No new error paths. `@ExceptionHandler` methods on both controllers are
class-scoped, not path-scoped, so they continue working unchanged under the new
mapping.

## Testing

Existing test suites (unit + Spring MockMvc + Testcontainers-backed end-to-end +
Angular unit/integration specs) are updated to assert the `/v1` paths and continue
to serve as the verification — no new tests are needed since this is a pure
path-rename with unchanged behavior. `mvn clean verify` (backend) and the frontend
test/build commands must pass before this is considered done.
