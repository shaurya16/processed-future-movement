# ingestion-service

Spring Boot service that reads the System A fixed-width file (`Input.txt`) using the
parser in [`common`](../common/) and publishes one Kafka event per transaction record
to the `future-transactions` topic.

Stands in for the "Kafka streaming input instead of file" scenario called out in the
requirements — this service is the producer side of that pipeline.

Design decisions: [docs/superpowers/specs/2026-08-09-ingestion-service-design.md](../docs/superpowers/specs/2026-08-09-ingestion-service-design.md).

## Running locally

Run these from the **repo root**:

```bash
docker compose up -d kafka                                    # starts a local Kafka broker on localhost:9092
mvn -q -DskipTests install                                     # builds & installs `common` (and all modules) to the local repo
INGESTION_FILE_PATH="$PWD/sample-data/Input.txt" mvn -pl ingestion-service spring-boot:run
```

Note: don't pass `-am` to the `spring-boot:run` step. `-am` pulls the root aggregator POM
and `common` into the reactor, and `spring-boot:run` tries to run on every reactor project
— including pom-packaged ones with no main class — which fails with "Unable to find a
suitable main class". Running `install` first publishes `common` to the local `.m2` repo,
so `ingestion-service` can build against it without `-am`.

By default it reads `sample-data/Input.txt`, resolved **relative to
`ingestion-service`'s working directory** (not the repo root) when run via
`spring-boot:run` — so the `INGESTION_FILE_PATH` override above (an absolute path) is
required unless you `cd` into `ingestion-service` first and the file happens to live there.

`INGESTION_FILE_PATH` is a **host** path here. Under Docker Compose the equivalent knob
is `PFM_INPUT_FILE` and takes a *container* path; the two are deliberately separate names
so an exported value from this dev loop cannot follow you into `docker compose up`.

In a separate terminal, once the service is up:

```bash
curl -X POST http://localhost:8081/api/v1/ingest
```

## API

- `POST /api/v1/ingest` — parses the configured file and publishes each record to Kafka.
  Returns a JSON body: `{fingerprint, totalLines, published, skipped, errors, cached}`.
  Calling it again for the same (unchanged) file returns the cached result without
  republishing (`cached: true`).
- `POST /api/v1/ingest?force=true` — bypasses the cache and republishes even if this exact
  file was already ingested.
