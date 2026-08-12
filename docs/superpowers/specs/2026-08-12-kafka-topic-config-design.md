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
| Java (both services) | New `common/src/main/resources/pfm-defaults.yml`: `pfm.topic: ${PFM_TOPIC:future-transactions}` | Both `application.yml` files import it (`spring.config.import: classpath:pfm-defaults.yml`) and reference `${pfm.topic}` instead of owning their own literal |
| Docker Compose | One `x-topic: &pfm-topic future-transactions` YAML anchor at the top of `docker-compose.yml` | `ingestion-service`, `processing-service`, and `wait-for-topic` each get `PFM_TOPIC: *pfm-topic` in their `environment:` block |
| Kubernetes | New `k8s/01-topic-config.yaml` ConfigMap (`data.PFM_TOPIC: future-transactions`) | `ingestion-service.yaml`, `processing-service.yaml`'s main container, and its `wait-for-topic` initContainer all pull `PFM_TOPIC` via `configMapKeyRef` |

Both `application.yml` files change from an independent literal:

```yaml
ingestion:
  topic: future-transactions
```

to a reference into the shared defaults file:

```yaml
spring:
  config:
    import: classpath:pfm-defaults.yml

ingestion:
  topic: ${pfm.topic}
```

(and the `processing:` equivalent in `processing-service`, same import line).

### Revision history: this section originally specified `${PFM_TOPIC}` with no default

The first version of this design made the property required with no
fallback (`topic: ${PFM_TOPIC}`), reasoning that Spring's placeholder
resolution would throw at startup if `PFM_TOPIC` was unset — fail fast and
loud, never a silently empty topic. **That assumption was wrong**, confirmed
empirically during implementation:

- For `@ConfigurationProperties`-bound records (`IngestionProperties`,
  `ProcessingProperties`), Spring Boot's relaxed binder resolves placeholders
  with `ignoreUnresolvablePlaceholders = true`. An unresolved `${PFM_TOPIC}`
  is bound as the literal string `"${PFM_TOPIC}"`, not thrown.
- `ingestion-service`: `KafkaAdmin`'s topic-creation failure (the invalid
  literal topic name) is logged at ERROR and swallowed by default — the
  Spring context finishes starting, Tomcat serves traffic, and
  `/actuator/health/readiness` reports `UP`. This is exactly the silent
  misconfiguration the whole task exists to prevent.
- `processing-service`: the context also starts successfully; Kafka Streams'
  background thread dies asynchronously on the same invalid-topic error, and
  `/actuator/health/readiness` does correctly flip to `DOWN` — a real signal,
  but not a startup-time failure, and easy to miss outside a system (like
  k8s) that gates traffic on that probe.

A zero-code fix existed for `ingestion-service` alone
(`spring.kafka.admin.fail-fast: true`, verified to abort context refresh),
but `processing-service` has no equivalent — it deliberately never creates
the topic itself (see `k8s/README.md`), so there is no admin-side hook to
make fail-fast. Reaching a genuine startup failure for `processing-service`
would have required a hand-rolled validation in `src/main` (e.g. a
`@PostConstruct` check), touching Java code and creating an asymmetric fix
between the two services.

### Why the shared-defaults-file approach instead

The shared classpath resource sidesteps the whole problem rather than
patching around it: if `ingestion-service` and `processing-service` both
resolve their topic from the exact same imported file, they cannot disagree
— whether or not `PFM_TOPIC` is ever set in a given environment. Forgetting
to set `PFM_TOPIC` stops being a failure mode at all: both services
consistently fall back to the same value from the same source. This is a
stronger guarantee than "fails loudly if forgotten" — it makes the forgotten
case harmless by construction.

It also happens to provide a genuine, framework-guaranteed fail-fast for a
different (and arguably more useful) failure mode: `spring.config.import`
with a non-optional (`classpath:`, not `optional:classpath:`) location
throws `ConfigDataResourceNotFoundException` and aborts context startup if
the resource is missing — verified empirically (renaming the file out of
`common`'s resources and rebuilding produces "APPLICATION FAILED TO START:
Config data resource ... does not exist" and a non-zero exit). That covers
a real packaging defect (the resource not making it into `common`'s jar)
symmetrically for both services, with no `src/main` Java changes — only one
new YAML resource file in `common`.

`PFM_TOPIC` remains meaningful as a deploy-time override (a different topic
name for staging vs. prod is legitimate 12-factor config) — Docker Compose
and Kubernetes still set it explicitly for that purpose. It's just no longer
load-bearing for avoiding drift between the two services.

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

- No README changes needed for the standalone `mvn -pl <service>
  spring-boot:run` workflows in `ingestion-service/README.md` /
  `processing-service/README.md` — behavior there is unchanged from today:
  no env var required, the topic resolves to `future-transactions` either
  way. (This is a change from the rejected first version of this design,
  which would have required documenting `PFM_TOPIC=future-transactions` on
  every standalone run command.)
- `k8s/README.md` gets a one-line note that the topic name comes from
  `k8s/01-topic-config.yaml`, settable per-deployment via `PFM_TOPIC`.

## Testing

`FullPipelineGoldenTest` sets `ingestion.topic` / `processing.topic`
explicitly as Spring properties (bypassing `application.yml`/its imports
entirely) and disables default config-file loading — unaffected, needs no
change.

The five `@SpringBootTest` classes that boot the full Spring context off the
unmodified classpath `application.yml` (`ingestion-service`:
`IngestionServiceApplicationTests`, `KafkaConfigTest`,
`IngestionEndToEndTest`; `processing-service`:
`ProcessingServiceApplicationTests`, `ProcessingEndToEndTest`) are expected
to need **no changes either**, unlike the rejected first version of this
design. `spring.config.import` still resolves normally when these tests
boot the default `application.yml`, `pfm.topic` still falls back to
`future-transactions` when `PFM_TOPIC` is unset (as it will be in a test
JVM), and `${pfm.topic}` resolves to the same value these tests always
implicitly relied on — nothing about their observable behavior changes.
This must still be confirmed by actually running `mvn test` per module
during implementation, not assumed from this reasoning alone.

### Manual verification

1. `mvn test` across modules — expected to pass unmodified per the above;
   confirms the reasoning holds in practice.
2. Run a service standalone without `PFM_TOPIC` set — confirm it starts
   cleanly and creates/consumes `future-transactions` (the point of this
   design: the forgotten-env-var case is now a non-event, not a failure).
3. Rename/remove `common/src/main/resources/pfm-defaults.yml`, rebuild, and
   confirm the affected service now fails fast at startup with
   `ConfigDataResourceNotFoundException` — the packaging-defect case this
   design does guarantee to catch.
4. `docker compose up -d --build` end to end — confirm
   ingestion → wait-for-topic → processing still sequences correctly with
   the anchor-derived value.
5. `kubectl apply -f k8s/` on a fresh `kind` cluster — confirm the ConfigMap
   is picked up by both deployments and the initContainer still gates
   correctly.

## Out of scope

- No `.java` source changes — only one new YAML resource file
  (`common/src/main/resources/pfm-defaults.yml`) and edits to existing
  `application.yml`/manifest/compose files.
- No Kustomize/templating introduced beyond the one new ConfigMap; k8s
  manifests otherwise stay plain YAML per the existing `k8s/README.md`
  design decisions.
