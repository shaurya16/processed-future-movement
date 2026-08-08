# Design: `common` module — fixed-width parsing + domain model

Status: Approved
Branch: `feature/common-fixed-width-parser`

## Context

This is the first buildable slice of the processed-future-movement project. The chosen
architecture (see [README.md](../../../README.md)) is:

```
Input.txt --> ingestion-service --> Kafka topic --> processing-service --> REST API --> frontend
                                                          (Kafka Streams
                                                           aggregation)
```

Both `ingestion-service` and `processing-service` need to work with the same
fixed-width record format and the same domain model, so that logic belongs in a shared
`common` module rather than being duplicated. `common` has no Spring or Kafka
dependency — it's a plain Java library, independently testable without either service
running.

Full field-position reference: [docs/file-spec.md](../../file-spec.md).

## Decisions made during brainstorming

1. **Build tool**: Maven, multi-module. A root `pom.xml` (packaging `pom`) aggregates
   `common`, `ingestion-service`, `processing-service` as modules with a shared parent
   for dependency/plugin versions. `frontend` and `k8s` stay outside the Maven tree.
2. **Model scope**: the domain model captures the **full record** — all ~27 fields from
   the file spec, not just the 9 fields the daily summary report currently uses. The
   shared Kafka event contract should carry the full source record so a later
   requirement needing e.g. commission or transaction price doesn't require touching
   the parser again.
3. **Parsing strategy**: annotation-driven, reflection-based mapping — a
   `@FixedWidthField(start, length)` annotation on record fields, read generically by
   `FixedWidthRecordParser.parse(line, lineNumber, Class<T>)`. Chosen over a purely
   hand-written positional parser specifically because it's reusable for any future
   fixed-width record type, not just `FutureTransaction` — directly serving the
   requirements' explicit call to focus on code reusability.
4. **Sign/decimal composite fields**: quantities (sign + value) and money fields
   (value + implied decimals + D/C indicator + currency) are mapped as **raw
   sub-fields** by the annotation-driven parser (e.g. `quantityLongSign` +
   `quantityLongRaw` as two separate `@FixedWidthField` strings), then assembled into
   final typed values by a separate `FutureTransactionFactory` step. This keeps the
   annotation itself simple (one annotation = one positional substring, no cross-field
   logic) at the cost of a second explicit assembly step.
5. **Error handling**: two-level API.
   - `parse(line, lineNumber)` is strict — throws `FixedWidthParseException(lineNumber,
     rawLine, reason)` on any failure. Useful for direct/simple use and unit tests.
   - `parseAll(lines)` is resilient — skip-and-collect. It never throws; it returns
     both the successfully parsed records and a structured list of parse errors, which
     is the raw material for an explicit error report. `common` guarantees the *data*
     needed for that report (line number, raw line, reason) but does not decide where
     the report is delivered (console log, CSV file, Kafka DLQ topic) — that's an
     `ingestion-service` decision, out of scope for this slice.

## Components

- **`FieldPositions`** — start/end offset constants for every field, transcribed 1:1
  from `docs/file-spec.md`. Single source of truth for byte positions; nothing else in
  the module hardcodes an offset.
- **`@FixedWidthField(start, length)`** — annotation marking a positional substring on
  a record field. `start`/`length` are 1-indexed per the file spec's own convention (the
  parser converts to 0-indexed `substring` calls internally).
- **`RawFutureTransaction`** — a record with one `String` field per raw fixed-width
  field (all ~27 spec fields, with quantities/money fields split into raw sign/value/DC
  sub-fields as above). Purely positional extraction, no type conversion or business
  interpretation yet.
- **`FixedWidthRecordParser`** — generic parser: `<T> T parse(String line, int
  lineNumber, Class<T> type)`. Reads `@FixedWidthField` annotations off any record type
  via reflection, extracts + trims each substring, constructs the record. Throws
  `FixedWidthParseException` if the line is too short for a field's declared offsets or
  construction otherwise fails.
- **`FutureTransactionFactory.from(RawFutureTransaction raw)`** → `FutureTransaction` —
  applies the quantity sign fields (blank/`+` = positive, `-` = negative) to produce
  signed `long` quantities; scales each money field's raw digits by its field-specific
  implied decimal count (fees/commission = 2 decimals, transaction price = 7 decimals)
  and applies its D/C indicator to produce signed `BigDecimal` values; parses
  `CCYYMMDD` date fields to `LocalDate`. Throws `FixedWidthParseException` (same family)
  on conversion failure — e.g. non-numeric quantity, unparseable date.

  **Assumption flagged for implementation**: every money field in the 717-line sample
  carries a `D` indicator; there is no `C` example to confirm the sign convention from
  data. The standard accounting reading (`D` = debit = negative, `C` = credit =
  positive) is assumed. Since none of the money fields feed the daily summary report
  (only the signed quantities do — see "Fields used by the daily summary report" in
  [docs/file-spec.md](../../file-spec.md)), an incorrect assumption here doesn't affect
  the report output; it only affects the fidelity of the full domain model. Worth a
  second look against the File Specification PDF's field description if `common` is
  ever extended to expose these fields.
- **`FutureTransaction`** — final typed domain record: `String` identifiers, `LocalDate`
  dates, signed `long` quantities, signed `BigDecimal` money fields, `char`
  buy/sell and open/close codes. No raw strings for anything that has a real type.
- **`ParseError(int lineNumber, String rawLine, String reason)`** — the row shape for
  the error report.
- **`ParseResult(List<FutureTransaction> records, List<ParseError> errors)`** and
  **`FixedWidthRecordParser.parseAll(List<String> lines)`** — skip-and-collect batch
  entry point. Parses every line independently; a failure on one line (at either the
  positional-parse step or the factory-conversion step) is caught and appended to
  `errors`, and parsing continues with the next line.

## Data flow

1. Caller (eventually `ingestion-service`) supplies `List<String>` — reading the file
   itself is **not** `common`'s responsibility, keeping the library usable from a batch
   file read today and from any other line source later.
2. Per line: `FixedWidthRecordParser.parse(line, lineNumber, RawFutureTransaction.class)`
   extracts every annotated substring.
3. `FutureTransactionFactory.from(raw)` converts to the final typed `FutureTransaction`.
4. `parseAll` wraps steps 2–3 per line in a catch, routing failures to `ParseError`
   entries instead of propagating, and returns the combined `ParseResult`.

## Testing (TDD)

- Fixtures are real lines pulled from `sample-data/Input.txt` (not hand-typed synthetic
  strings) — e.g. a buy record for client `4321`/`SGX`/`NK`, a sell record, a record
  with a negative sign — so tests are anchored to the actual System A export format,
  not an idealized version of it.
- `FixedWidthRecordParser` unit tests: correct substring per field, trimming behavior,
  throws `FixedWidthParseException` on a truncated line.
- `FutureTransactionFactory` unit tests: sign application (positive/negative/blank) on
  quantities, decimal scaling per field's implied decimal count, D/C application on
  money fields, date parsing.
- `parseAll` unit tests: all-good input, some-bad input (bad line doesn't affect
  surrounding good ones), all-bad input; error entries carry the correct line number
  and reason.
- One golden integration-style test: `parseAll` over the entire 717-line
  `sample-data/Input.txt`, asserting exactly 717 records and 0 errors. Doubles as a
  regression guard — a future edit to `FieldPositions` that breaks alignment fails this
  test immediately.
- Test-first order: start with the smallest failing test (extract one known field from
  one real line), get it green, then broaden field by field.

## Out of scope for this slice

- File I/O (reading `Input.txt` from disk) — belongs to `ingestion-service`.
- Kafka publishing, topic/schema design, DLQ delivery mechanism — `ingestion-service`
  design, next slice.
- Kafka Streams aggregation, REST API — `processing-service` design, later slice.
- Angular frontend, Kubernetes manifests — later slices.
