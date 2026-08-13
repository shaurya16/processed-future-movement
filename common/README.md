# common

Shared library used by both `ingestion-service` and `processing-service`, so the
fixed-width parsing logic and domain model exist in exactly one place.

Two things live here, and only one of them is genuinely shared:

**The cross-service contract** (`com.pfm.common.domain`) — used by both services, so
they cannot drift apart on the wire format:
- `FutureTransaction` — the typed record published to Kafka as JSON
- `ReportKey` — client info + product info, owning both `encode()` and `decode()`
- `NetPosition` — the aggregate value held in the state store
- `TransactionHeaders` — the Kafka record header names

**A general-purpose fixed-width parser** (`com.pfm.common.fixedwidth`, plus
`FieldPositions`, `RawFutureTransaction`, `FutureTransactionFactory`,
`FutureTransactionParser`) — an annotation-driven, position-based parser driven by the
field table in [`docs/file-spec.md`](../docs/file-spec.md). Today only
`ingestion-service` uses it; it sits here because it is record-type-agnostic by design,
not because both services need it.

`common` deliberately has no Spring and no Kafka dependency — the parser is wired up as
a bean by the service that consumes it. Note that the Jackson configuration for the
Kafka JSON payload is therefore *not* here; each service configures its own.

`common` also ships `src/main/resources/pfm-defaults.yml`, a shared Spring Boot
config-data resource holding the Kafka topic-name default, imported by both
`ingestion-service` and `processing-service` via `spring.config.import`. `common`
itself has no Spring dependency, but it's the one classpath both services already
share, which is why the file lives here rather than in a new module.
