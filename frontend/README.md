# frontend

Angular app that calls `processing-service`'s REST API to display the daily summary
table and download it as `Output.csv`.

Design decisions: [docs/superpowers/specs/2026-08-11-frontend-design.md](../docs/superpowers/specs/2026-08-11-frontend-design.md).

## Running locally

Run these from the **repo root** first, so `processing-service` is up:

```bash
docker compose up -d kafka                                    # starts a local Kafka broker on localhost:9092
mvn -q -DskipTests install
mvn -pl processing-service spring-boot:run
```

Then, in a separate terminal, from `frontend/`:

```bash
npm install
npm start
```

Open `http://localhost:4200`. The dev server proxies `/api/*` requests to
`processing-service` on `localhost:8082` (see `proxy.conf.json`).

## Proxying

Two upstreams are proxied, both exact-matched before the general `/api/` rule so
routing (not application policy) enforces the split:

- `GET /api/v1/ingest/status` → `ingestion-service` (`localhost:8081` in dev; see
  `nginx.conf.template`'s `INGESTION_SERVICE_UPSTREAM` in the container) — source-file
  provenance for the panel above the table.
- everything else under `/api/` → `processing-service` (`localhost:8082` in dev;
  `PROCESSING_SERVICE_UPSTREAM` in the container) — the report and its CSV download.

`POST /api/v1/ingest` is deliberately **not** reachable through this origin: it falls
through the exact-match status route into the general `/api/` rule, lands on
`processing-service`, and 404s there. The UI is a viewer and cannot trigger ingestion —
enforced by routing, not by hiding a button.

## Persisted preferences

Three `localStorage` keys survive a reload:

- `pfm.theme` — Auto / Light / Dark, cycled by the theme toggle.
- `pfm.autoRefresh` — whether the 5-second poll is running.
- `pfm.visibleColumns.v2` — which of the 17 report columns are shown.

## Auto-refresh

The report polls `GET /api/v1/report` every 5 seconds while auto-refresh is on. The
poll pauses while the tab is hidden (`visibilitychange`) and refetches immediately on
becoming visible again, rather than waiting out a possibly-stale interval. A failed
refresh keeps the last good data on screen with a stale badge instead of blanking the
table; only the very first load treats an error as fatal.

## Expiry badges

Days-to-expiry are measured against each row's **last trade date**, not wall-clock
"today" — the sample data's trades and expiries are all in 2010, so comparing to today
would badge every row "expired" and say nothing useful about the data itself.

## Running the container image standalone

The `pfm/frontend:local` image (see [k8s/README.md](../k8s/README.md) for how to build
it) requires a `PROCESSING_SERVICE_UPSTREAM` env var — a full scheme+host+port URL for
`processing-service`, e.g. `http://processing-service:8082` — which nginx uses as the
proxy target for `/api/*`. `docker-compose.yml` and `k8s/frontend.yaml` both set this
already; if you `docker run` the image directly without it, the container fails to
start with an nginx `emerg` error.

## Testing

```bash
npm test
```
