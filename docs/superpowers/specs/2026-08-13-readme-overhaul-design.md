# Design: root README overhaul — the reviewer's entry point

Status: Approved
Branch: `docs/readme-overhaul` (off main @ 030cd84)

## Problem

The root README was written incrementally, one slice at a time, and reads like a
build log: a per-module "Status" list of what is done. That served the project
while it was being built. It does not serve the person the repo now exists for —
a reviewer who has never seen it and needs to understand the architecture, run
it, and find the reasoning behind the decisions.

Three specific gaps:

- **The reasoning is invisible.** Nine design docs in `docs/superpowers/specs/`
  carry the rationale behind every major decision. Nothing in the README links to
  them, so the strongest artifact in the repo is the hardest to find.
- **The brief is not traceable.** Nothing maps the stated requirements
  (`Client_Information` composition, `sum(QUANTITY LONG - QUANTITY SHORT)`,
  `Output.csv`, the REST API) to where they are met.
- **A known foot-gun is undocumented.** The report is a running aggregate. Ingesting
  a second file adds to the existing totals instead of replacing them, because a
  different file produces different content-derived `transactionId`s that the dedup
  store has never seen. This is designed behaviour and it surprises everyone.

## Decisions

1. **Structure: twelve sections, ordered for a reviewer's reading path** —
   architecture, startup, usage, assumptions and rationale, scalability, endpoints,
   tech stack, requirements traceability, testing, CSV/JSON divergence, known
   limitations, design decisions.

2. **Summarise and link; never duplicate the specs.** The specs are the source of
   truth for rationale. Any section that outgrows its budget loses depth to a link
   rather than gaining paragraphs. Scalability is capped at three paragraphs on the
   grounds that a reviewer reads three paragraphs and skims two pages.

3. **Keep the ASCII overview, add a mermaid diagram below it.** The ASCII line
   answers "what is this" in two seconds and survives any renderer. The mermaid
   diagram carries the detail — both state stores by name, the `transactionId`
   header, the message key, partition counts. GitHub renders mermaid natively.

4. **Document the real message key, not the report columns.** The key is
   `ReportKey.encode()` — eight pipe-delimited fields, not
   `Client_Information|Product_Information`. The two report columns are *derived*
   from those eight (`ReportKey.clientInformation()`, `productInformation()`), because
   the parser trims each field before concatenation, making the sub-field boundaries
   variable-width and unrecoverable from the joined string. Describing the key as the
   two columns would be wrong and would obscure why aggregation is partition-local.

5. **Title the section "Assumptions and design rationale", not "Assumptions".**
   The trigger model (`POST /api/v1/ingest` rather than a `CommandLineRunner` or a
   directory watcher) is a deliberate choice with a documented rationale — scheduling
   policy belongs outside the service. Filing it under "assumptions" reads as a
   caveat and undersells it. Genuine assumptions are listed separately and marked as
   such.

6. **Fold the "Status" section into the module table.** Every module is done; a
   per-module completion checklist reads as in-progress project notes rather than a
   finished deliverable. Its feature-level detail moves into the table's
   Responsibility column.

7. **Add the `./sample-data` bind mount to `docker-compose.yml`.** The README needs
   to tell a reviewer how to try their own file. Without the mount that requires an
   image rebuild, which is not a reasonable ask. Mounted read-only at
   `/app/sample-data`, shadowing the copy baked into the image, with
   `INGESTION_FILE_PATH` set explicitly so the wiring is visible in the compose file
   rather than implied by an application default.

## Verified before writing

Every factual claim was checked against the source rather than carried over from
the previous README:

| Claim | Source |
|---|---|
| `POST /api/v1/ingest`, `GET /api/v1/ingest/status` | `IngestionController.java` — the ingest endpoint *is* versioned |
| `GET /api/v1/report`, `/api/v1/report/csv`, 503 on not-ready | `ReportController.java` |
| `seen-transaction-ids`, `net-quantity-store` | `DedupProcessor.java`, `AggregationTopology.java` |
| `transactionId = sha256(fileContentHash + ":" + lineNumber)` | `TransactionIdBuilder.java` |
| 3 partitions, `acks=all`, `enable.idempotence=true` | `KafkaTopicConfig.java`, `KafkaProducerConfig.java` |
| Java 21, Spring Boot 3.5.4, Kafka clients 3.9.1, broker 3.9.2 | root `pom.xml`, module poms, `docker-compose.yml` |
| Angular 21.2, Tailwind 4.3.3, TypeScript 5.9, Vitest 4.0.8 | `frontend/package.json` |
| D = debit on all three money fields, no `C` anywhere | 717/717 records are `DDD` at positions 86/102/118 of `sample-data/Input.txt` |
| 717 records, 176 bytes | `sample-data/Input.txt`, `docs/file-spec.md` |
| CSV is exactly three columns | `ReportController.java:31` |

The D-is-debit assumption is worth calling out: it is confirmed to be *consistent*
across the sample, but not *verified* — there is no `C` record anywhere in the
sample, so the accounting convention is inferred. That became load-bearing once fees
were surfaced in the UI; before then it affected nothing the report displayed.

## Out of scope

- Rewriting the per-module READMEs. They are accurate and the root README links to
  them.
- Any change to application code. The only non-documentation change is the
  `docker-compose.yml` bind mount, which exists to make a documented workflow
  possible.
