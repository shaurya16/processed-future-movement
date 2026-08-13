# Follow-ups

Known issues that are **not** fixed, recorded deliberately rather than left implicit.

These came out of a structured review of the finished codebase — one pass per module
plus one over the cross-cutting seams. The review produced roughly fifty findings. Nine
were fixed (see the [branch review wave](#what-was-fixed-instead) below); the rest are
here, because each one needs a design decision rather than a mechanical edit, and making
those calls unilaterally at the end of a project is how you get changes nobody asked for
in code nobody has time to re-review.

Nothing in this list is currently breaking. Every item was verified against the code
rather than assumed, and file references are given so each can be checked directly.

---

## Architecture

**The CSV builder does not exist** — `processing-service/.../ReportController.java:29-44`

The header literal, the column projection, the row joining and the line terminator all
live in a `StringBuilder` inside the web adapter. `CLAUDE.md` describes "the CSV builder
projects only these three columns, deliberately" — there is no such unit. Consequences:
the project's central output contract is only reachable through `MockMvc`, and the header
string is duplicated between the controller and its test, so "a test pins the header" is
really two independent literals that happen to agree.

*Fix:* extract `ReportCsvBuilder.build(List<ReportEntry>)` with the header as a constant,
leaving the controller to do status, headers and body. That also gives the escaping issue
below a single place to live.

**The REST layer knows about Kafka Streams** — `processing-service/.../ReportService.java`

`com.pfm.processing.report` imports `StreamsBuilderFactoryBean`, `KafkaStreams.State`,
`StoreQueryParameters`, `QueryableStoreTypes` and `KeyValueIterator`, and reaches into
`AggregationTopology` for the store name. `ReportService` does four jobs in sixteen lines:
readiness gating, Interactive Query mechanics, key decoding, and DTO mapping plus sorting.

It is small enough to read today. The cost is that any change to *how* the store is
queried — stale reads, cross-instance routing, the exception handling below — lands in
the REST package rather than beside the topology that owns the store.

*Fix:* a `NetPositionStore` in `streams` exposing `isReady()` and `snapshot()`, leaving
`ReportService` to map, sort and throw.

**The root POM parents only one of its three modules** — `pom.xml` vs the service POMs

The root aggregates `common`, `ingestion-service` and `processing-service`, but both
services parent to `spring-boot-starter-parent` directly. So the root's
`dependencyManagement` and `pluginManagement` reach `common` alone, and `java.version`,
the Testcontainers BOM, the `testcontainers.docker.api.version` property and the Surefire
configuration are each written out twice. `0.1.0-SNAPSHOT` appears as a literal in seven
places. The design spec calls for "a shared parent for dependency/plugin versions", so
this is a drift from the stated design, not a deliberate choice.

*Fix:* make the root the parent of all three and import `spring-boot-dependencies` in its
`dependencyManagement` — the standard Boot pattern when a project needs its own parent.

---

## Correctness and robustness

**A rebalance returns 500 where the contract promises 503** — `ReportService.java:29-44`

`kafkaStreams.state() != RUNNING` is checked, and *then* the store is queried. Both
`kafkaStreams.store(...)` and `store.all()` can throw `InvalidStateStoreException` — a
rebalance starting between the two lines, or a task whose store is still restoring while
the app already reports `RUNNING`. Nothing catches it. The README and the design spec both
promise 503 for "store not ready", and this is precisely the transient case 503 exists for.

*Fix:* catch it and rethrow as `StoreNotReadyException`, or add an `@ExceptionHandler`.

**CSV values are never escaped** — `ReportController.java:33-37`

The design spec rejected a CSV library on the grounds that the two text columns "are
fixed concatenations of positional codes and can't contain commas or quotes". The codes
are not fixed — they are whatever bytes the source system put in those slices, trimmed.
`symbol` already carries punctuation in the sample data (`NK.`), and nothing between the
parser and the writer validates the character set. A comma in `symbol` or `clientNumber`
would silently produce a mis-columned row, and the golden test would not catch it because
it runs on the one sample file.

*Fix:* the cheap honest option is a validation, not a library — either quote-if-needed in
the extracted builder, or fail loudly if a component contains `,` `"` or a newline.

**One malformed byte fails the entire file** — `IngestionService.java:92`

`Files.readAllLines(path)` without a charset is UTF-8 with `CodingErrorAction.REPORT`, so
a single non-UTF-8 byte anywhere throws and discards all 717 records — contradicting the
design's central promise that one bad line never blocks the rest of the file. Relatedly,
`FixedWidthRecordParser` slices by `char` index while the record is defined in *bytes*;
those diverge for any non-ASCII byte, silently misaligning every field after it.

*Fix:* read as ISO-8859-1. It is byte-per-char and never throws, which makes char offsets
identical to byte offsets by construction and downgrades an unexpected byte from a
whole-run failure to a per-field parse error.

**A blank optional field discards a good record** — `FutureTransactionFactory.java`

`parseLong` on a blank fee, `parseDebitCredit` on a blank indicator, or `parseDate` on a
zero-filled date each throw, so the whole line is dropped. None of `exchBrokerFee`,
`clearingFee`, `commission`, `transactionPrice` or `transactionDate` feeds the summary —
only the two quantities and the eight key fields do. So a record with perfectly good
quantities can be excluded from a financial total because an optional money field was
blank. Blank optional numerics are common in real fixed-width exports; the sample
happening to populate all of them is not the format's contract.

*Fix:* treat blank money/price fields as zero while keeping non-numeric *content* fatal.
Resist a blanket relaxation — the point is to match strictness to the field's importance,
which is currently inverted.

**A KPI compares a per-run counter against a cumulative one** — `frontend/.../kpi-row.ts:62-69`

`published` comes from `IngestionStatus`, which reports the **last ingestion run only**.
`transactions` sums `tradeCount` across the aggregate, which is **cumulative across every
ingest**. They are compared for equality, and any difference renders a red
"⚠ N published — M aggregated" warning. In the exact workflow the README warns about —
ingesting a second file without `docker compose down -v` — the tile claims records were
lost when nothing was. The code comment correctly identifies the filtering caveat but not
the scope caveat.

*Fix:* warn only when the aggregate is *less* than `published` (the only direction that
indicates loss), or restate the tile so the two scopes are not implicitly equated.

---

## Error model and API surface

**`ParseError` is repurposed as a Kafka-send-failure carrier** — `IngestionService.java:119-136`

`new ParseError(-1, key, "Kafka send failed: ...")` puts a Kafka message key into a field
named `rawLine` and a magic `-1` into `lineNumber`. Cache correctness then depends on
recognising that `-1`. Three problems: the type is imported from `common`, so a shared
module implicitly owns ingestion's infrastructure-error encoding; the field names lie
about their contents; and `IngestionResult.skipped` conflates "unparseable line" with
"parsed but could not be sent" — two things an operator needs to tell apart.

*Fix:* an ingestion-local error type with an explicit kind (`PARSE` / `SEND` / `ABORTED`).

**Diagnostics are collected and then discarded** — `KafkaPublishException.java`

The exception is built with the full failure list, and the handler returns only
`Map.of("error", e.getMessage())`. `failures()` is never called anywhere in the repo. A
caller receiving a 502 learns "all 717 failed" but nothing about why — and since the
result is not cached, those reasons are unrecoverable.

*Fix:* include them in the 502 body, or delete the field. Carrying diagnostic state
nobody reads is worse than either.

**The ingest response exposes what the status endpoint deliberately hides** — `IngestionService.java:148`

`IngestionStatus` returns an error *count* only, with a test documenting the rule: "raw
lines contain client data and are never exposed". But the ingest response embeds
`List<ParseError>`, each carrying `rawLine` — the complete source record, client number
and all. With no authentication anywhere in the stack, the same data one endpoint refuses
to show is returned in full by the endpoint next to it.

*Fix:* return `lineNumber` + `reason` on the wire and log `rawLine` server-side; or state
explicitly that the demo wants it, so the inconsistency reads as a decision.

**Configuration is unvalidated** — `IngestionProperties.java`

Neither `filePath` nor `topic` is constrained, so a missing `ingestion.file-path` gives an
NPE on the first request rather than a startup failure.

*Fix:* `@Validated` with `@NotBlank`, so misconfiguration fails the context refresh with a
readable message.

**The error envelope is untyped on both sides** — both controllers, `frontend/.../report.service.ts`

Both services independently return `Map.of("error", …)`, and the frontend depends on that
shape through an inline `err.error?.error ?? …` with no declared interface. The one error
contract in the system is the only part of the API model that is neither typed nor named
at either end.

*Fix:* a small `ApiError` record per service and a matching TS interface.

---

## Frontend polish

- **State changes are not announced to assistive tech** — the report's loading, error and
  stale states swap silently; the filter bar's `aria-live` is the only live region on the
  page, and the table has neither a caption nor an accessible name.
- **Escape does not dismiss the column picker for the user who just opened it**
  (`column-picker.ts`) — the handler is bound to the panel, but focus is on the toggle
  button, a sibling. There is also no click-outside dismissal. The existing test dispatches
  the event directly on the panel, so it verifies the binding rather than the behaviour.
- **"updated Ns ago" freezes when auto-refresh is off** (`refresh-control.ts`) — under
  zoneless change detection the view only re-renders when a signal it reads changes, and
  with polling off `lastLoadedAt` never changes. That is exactly when the freshness
  readout matters most.
- **The row-change flash fires when a filter is widened** (`report-table.ts`) — the effect
  reads the filtered projection, and any key absent from the previous snapshot counts as
  changed, so clearing a filter flashes every newly visible row as if the data had updated.

---

## Test coverage

- **Fixtures are copy-pasted.** The 32-argument `FutureTransaction` literal appears in
  three test classes; a `row()` builder appears in four frontend spec files plus a fifth
  inline copy. Adding a field means editing all of them.
- **Two deliberate branches are untested:** the dedup processor's missing-header path
  (forward-and-warn — the one place the dedup guarantee is intentionally waived, so it
  deserves a test that stops it being quietly inverted), and the 503 response from
  `/api/v1/report/csv`, which is the non-obvious one because the endpoint declares
  `produces = "text/csv"` while the handler returns a `Map`.

---

## What was fixed instead

For contrast, the nine issues from the same review that *were* fixed, because each had a
clear right answer and a test that could prove it:

| Fix | Why it could not wait |
|---|---|
| Poll tick hijacked the initial 503 retry loop | Showed the error screen on the default cold-start path |
| `force=true` cached a partial ingestion | Could mark a half-ingested file complete forever |
| Repartition topic between dedup and aggregation | Doubled the write path; two docs claimed it did not exist |
| `transactionId` header declared twice | On drift, dedup silently stops and totals double |
| Dev proxy exposed `POST /api/v1/ingest` | `ng serve` could trigger a real ingest |
| Duplicated SHA-256 helpers, duplicated serde config | Two copies that had already diverged in behaviour |
| Misleading names (`unscaledDecimal`, `instance()`, filter labels) | One was a visible UI inconsistency |
| Missing `Input.txt` drift guard | Four unguarded copies of the input contract |
| `.gitattributes` line-ending declaration | Every fresh clone arrived with four modified files |
