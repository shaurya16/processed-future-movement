# frontend

Angular app that calls `processing-service`'s REST API to display the daily summary
table and download it as `Output.csv`.

Design decisions: [docs/superpowers/specs/2026-08-11-frontend-design.md](../docs/superpowers/specs/2026-08-11-frontend-design.md).

## Running locally

Run these from the **repo root** first, so `processing-service` is up:

```bash
docker compose up -d
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

## Testing

```bash
npm test
```
