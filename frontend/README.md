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
