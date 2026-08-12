# Kafka topic name: single source of truth

## Problem

The Kafka topic name `future-transactions` is declared independently in four
places that must agree or the pipeline breaks:

- `ingestion-service/src/main/resources/application.yml` (`ingestion.topic`)
- `processing-service/src/main/resources/application.yml` (`processing.topic`)
- `k8s/processing-service.yaml` (`wait-for-topic` initContainer grep)
- `docker-compose.yml` (`wait-for-topic` service grep)

The Java code is already clean — both services read the value via
`@ConfigurationProperties` (`IngestionProperties`, `ProcessingProperties`),
so there are no hardcoded literals in `src/main`. The duplication is purely
in config.

No test covers drift between these four. `FullPipelineGoldenTest` looks like
it would, but it sets `ingestion.topic` / `processing.topic` explicitly as
Spring properties (bypassing `application.yml` entirely) and disables default
config-file loading via `spring.config.name`, so the `application.yml`
defaults are never exercised by any test.

Two ways this drifts, and how each fails:

- **Forgetting `processing.topic`** (or `ingestion.topic`): passes CI green,
  then fails silently in production — ingestion publishes to the new topic,
  processing consumes the old, empty one, and `GET /api/report` returns
  `200 []`, indistinguishable from "nothing ingested yet."
- **Forgetting the compose/k8s grep**: loud but confusing — the startup gate
  waits forever for a topic that's never created under that name, so
  `processing-service` never starts and the stack appears to hang.

## Approach

Collapse to one declaration per environment; everything else references it.
Prevention by construction, not a test that merely detects drift after the
fact.

| Environment | Single declaration | Consumers |
|---|---|---|
| Local (`mvn spring-boot:run`) | `PFM_TOPIC` exported on the command line, documented in each service's README | Spring reads it directly — no default to fall back on |
| Docker Compose | One `x-topic: &pfm-topic future-transactions` YAML anchor at the top of `docker-compose.yml` | `ingestion-service`, `processing-service`, and `wait-for-topic` each get `PFM_TOPIC: *pfm-topic` in their `environment:` block |
| Kubernetes | New `k8s/topic-config.yaml` ConfigMap (`data.PFM_TOPIC: future-transactions`) | `ingestion-service.yaml`, `processing-service.yaml`'s main container, and its `wait-for-topic` initContainer all pull `PFM_TOPIC` via `configMapKeyRef` |

Both `application.yml` files change from a literal:

```yaml
ingestion:
  topic: future-transactions
```

to a required placeholder, with no default:

```yaml
ingestion:
  topic: ${PFM_TOPIC}
```

(and the `processing:` equivalent in `processing-service`).

### Why not keep a matching default in both `application.yml` files?

That was considered (`topic: ${PFM_TOPIC:future-transactions}` in both) and
rejected: it still leaves the literal duplicated as a fallback, just one
layer down — the exact shape of problem this change exists to eliminate.
Dropping the default instead means Spring's placeholder resolution throws at
startup if `PFM_TOPIC` isn't set — fail fast and loud, never a silently
empty topic.

### Why an anchor for Compose but a ConfigMap for k8s?

A YAML anchor only resolves within a single YAML document/file. Compose's
duplication is entirely inside one file (`docker-compose.yml`), so an anchor
is sufficient and adds no new resource. Kubernetes' three references span
two separate manifest files (`ingestion-service.yaml`,
`processing-service.yaml`) plus an initContainer block, which an anchor
cannot reach across — a ConfigMap is the smallest mechanism that actually
gives k8s one real source of truth, and matches how the project already
manages k8s-level config (no Kustomize/templating currently in use, so this
is the first ConfigMap in `k8s/`).

### `wait-for-topic` scripts

Both the Compose service and the k8s initContainer currently grep for the
literal `future-transactions`. Each changes to read its own `PFM_TOPIC`
container env var instead:

```sh
until /opt/kafka/bin/kafka-topics.sh --bootstrap-server <broker> --list | grep -qx "$PFM_TOPIC"; do
  echo "waiting for $PFM_TOPIC topic..."
  sleep 2
done
echo "$PFM_TOPIC topic found, starting processing-service"
```

## Side effects

- `ingestion-service/README.md` and `processing-service/README.md` document
  standalone `mvn -pl <service> spring-boot:run` workflows that don't go
  through Compose or k8s. Each needs `PFM_TOPIC=future-transactions` added to
  that command line, the same way `INGESTION_FILE_PATH` is already handled
  for `ingestion-service`.
- `k8s/README.md` gets a one-line note that the topic name now comes from
  `k8s/topic-config.yaml` rather than being hardcoded in the manifest.

## Testing

`FullPipelineGoldenTest` and the pure-unit tests (`IngestionServiceTest`,
`KafkaConfigTest`'s topic-content assertion, `AggregationTopologyTest`,
`TransactionSerdeTest`) already set `ingestion.topic` / `processing.topic`
explicitly or use `"future-transactions"` as their own literal fixture —
unaffected by removing `application.yml`'s default.

However, five `@SpringBootTest` classes boot the full Spring context off the
unmodified classpath `application.yml` and currently rely on its default
silently supplying the topic — they don't set `ingestion.topic` /
`processing.topic` themselves:

- `ingestion-service`: `IngestionServiceApplicationTests`, `KafkaConfigTest`,
  `IngestionEndToEndTest`
- `processing-service`: `ProcessingServiceApplicationTests`,
  `ProcessingEndToEndTest`

Once the default is removed, these fail context startup (unresolved
`${PFM_TOPIC}` placeholder) unless `PFM_TOPIC` happens to be set in the
`mvn test` environment. Rather than depend on that, each test gets the topic
added to its existing explicit property mechanism — `properties = {...}` for
`IngestionServiceApplicationTests`, `KafkaConfigTest`, and
`ProcessingServiceApplicationTests`; a `registry.add(...)` line in the
existing `@DynamicPropertySource` method for `IngestionEndToEndTest` and
`ProcessingEndToEndTest`. This makes each test's dependency on the topic
name explicit rather than implicit, consistent with how
`FullPipelineGoldenTest` already does it.

### Manual verification

1. `mvn test` across modules — should be unaffected; confirms no hidden
   dependency on the old default.
2. Run a service standalone without `PFM_TOPIC` set — confirm it now fails
   fast at startup with a clear Spring placeholder-resolution error.
3. `docker compose up -d --build` end to end — confirm
   ingestion → wait-for-topic → processing still sequences correctly with
   the anchor-derived value.
4. `kubectl apply -f k8s/` on a fresh `kind` cluster — confirm the ConfigMap
   is picked up by both deployments and the initContainer still gates
   correctly.

## Out of scope

- No change to the Java code — it was already clean.
- No Kustomize/templating introduced beyond the one new ConfigMap; k8s
  manifests otherwise stay plain YAML per the existing `k8s/README.md`
  design decisions.
