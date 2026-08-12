# UI redesign: Tailwind, a real data table, and a richer report contract

## Problem

The UI shows three columns because the API returns three fields, and the API
returns three fields because the Kafka Streams grouping key throws the
underlying data away.

`GET /api/v1/report` returns:

```json
{ "Client_Information": "CL432100030001",
  "Product_Information": "CMEFUNK.20100910",
  "Total_Transaction_Amount": -215 }
```

Both strings are concatenations of four fields each, but
`FixedWidthRecordParser.parse` calls `.trim()` on every field before they are
joined, so the sub-field boundaries are **gone and variable-width**:

| `Product_Information` | exchange | group | symbol | expiry |
|---|---|---|---|---|
| `SGXFUNK20100910` (15 ch) | `SGX` | `FU` | `NK` | `20100910` |
| `CMEFUNK.20100910` (16 ch) | `CME` | `FU` | `NK.` | `20100910` |

`CL432100030001` cannot be reliably split back into client type / client number
/ account number / subaccount — the split point is genuinely ambiguous. So
per-field columns and per-field filters are **not achievable in the frontend**;
they require a backend change. This is the root cause, and everything else in
this spec follows from fixing it.

Three further gaps:

- **The aggregate value is a bare `Long`.** A row showing `0` is ambiguous:
  nothing happened, or `+500` and `-500` cancelled. There is no trade count, so
  `-215` could be one large trade or forty small ones, and the report cannot be
  reconciled against the 717-record input file.
- **No file provenance reaches the browser.** Path, size, last-modified, and
  ingest time exist only inside ingestion-service, which the frontend cannot
  reach: both `nginx.conf.template` and `proxy.conf.json` route `/api/*`
  exclusively to processing-service.
- **Any error destroys the table.** `ReportService` sets `status='error'` on any
  non-503 failure and `report.html` hides the table whenever status isn't
  `ready`. That is survivable for a manual page load; under a 5-second refresh
  poll a single blip would wipe the screen.

The UI is also unstyled beyond ~40 lines of hand-rolled CSS in `report.css`.

## Scope note: row counts are small

The 717-record sample file aggregates to **5 rows** — 4 distinct clients, 3
distinct products. That is inherent to the grouping, not a quirk of the sample.
So pagination and virtualization are deliberately excluded: the API returns the
whole store in one response regardless, so paging a payload already downloaded
buys nothing. Filtering and sorting are client-side over Angular computed
signals, which costs nothing at 5 rows and stays correct at thousands. If row
counts ever become genuinely large, the honest fix is server-side query
parameters, not client-side paging.

## Approach

Four slices, each independently shippable, in dependency order.

| Slice | Change | Touches |
|---|---|---|
| 1 | Structured report key + enriched aggregate | `common`, `ingestion-service`, `processing-service` |
| 2 | Ingestion status endpoint + narrow proxy route | `ingestion-service`, `frontend` config, `docker-compose.yml`, `k8s` |
| 3 | Tailwind v4 + validated design tokens | `frontend` |
| 4 | Table, filters, column picker, auto-refresh | `frontend` |

**The CSV output format does not change.** It is locked by a golden test, not
by convention — see Testing.

---

## Slice 1 — Structured report key and enriched aggregate

### `ReportKey` in `common`

The key format currently lives in two places that must agree: `KafkaKeyBuilder`
writes it, `ReportService` splits it on `|`. Collapse to one declaration,
mirroring how `common` already single-sources parsing:

```java
// common/src/main/java/com/pfm/common/domain/ReportKey.java
public record ReportKey(
        String clientType, String clientNumber, String accountNumber, String subaccountNumber,
        String exchangeCode, String productGroupCode, String symbol, LocalDate expirationDate) {

    public static ReportKey from(FutureTransaction tx);
    public String encode();                  // 8 fields, '|'-delimited, expiry as yyyyMMdd
    public static ReportKey decode(String encoded);

    public String clientInformation();       // clientType + clientNumber + accountNumber + subaccountNumber
    public String productInformation();      // exchangeCode + productGroupCode + symbol + yyyyMMdd
}
```

`clientInformation()` and `productInformation()` become **derived**, which
removes today's duplication — the CSV column values are computed from the same
source the key is.

`decode` must use `split("\\|", -1)` and reject anything that isn't exactly 8
parts, so a malformed key fails loudly rather than silently producing blank
columns. `KafkaKeyBuilder` is rewritten to delegate to `ReportKey.from(tx).encode()`.

The key gains fields, so the encoded string changes shape from
`client|product` (2 parts) to 8 parts. `Client_Information` and
`Product_Information` values themselves are unchanged.

### `NetPosition` replaces `Long` as the aggregate value

```java
// common/src/main/java/com/pfm/common/domain/NetPosition.java
public record NetPosition(
        long netQuantity,               // sum(quantityLong - quantityShort) — the CSV number
        long grossLong,                 // sum(quantityLong)
        long grossShort,                // sum(quantityShort)
        int tradeCount,
        LocalDate firstTransactionDate,
        LocalDate lastTransactionDate,
        Instant lastUpdatedAt,
        Map<String, BigDecimal> feesByCurrency) {   // each fee under its own currency code

    public static NetPosition empty();
    public NetPosition plus(FutureTransaction tx, Instant observedAt);
}
```

`feesByCurrency` accumulates all three fee amounts — `exchBrokerFee`,
`clearingFee`, `commission` — each keyed by its own currency field, so no two
currencies are ever added together. It exists to feed the fee KPI tile; per-fee
columns remain out of scope.

`AggregationTopology` changes its `.aggregate(...)` seed to `NetPosition.empty()`
and its accumulator to `plus(...)`, materialized with a JSON `NetPositionSerde`
following the existing `TransactionSerde` pattern. `netQuantity` continues to be
`sum(quantityLong - quantityShort)`, so the reported number is bit-for-bit what
it is today.

**`lastUpdatedAt` sourcing.** The `Aggregator` interface has no
`ProcessorContext`, so the observation time must be supplied from outside. A
`java.time.Clock` is passed into `AggregationTopology.build(...)` and the
aggregator calls `clock.instant()`.

An earlier draft had `DedupProcessor` forward a
`TimestampedTransaction(FutureTransaction, long observedAtMs)` wrapper instead.
The `Clock` reaches the same outcome for less: no wrapper record, no second serde,
no change to the topology's generic types — and it makes the timestamp
deterministic in tests via `Clock.fixed`, which the wrapper did not.

Caveat to accept explicitly: `lastUpdatedAt` is *processing* time, not event
time, so a state-store rebuild re-stamps every row with the rebuild time. That
is correct for its purpose — "when did this row last change in this store,"
which is what drives the changed-row highlight — but it is not a business
timestamp. `lastTransactionDate` is the event-time field.

### `ReportEntry` grows, backward-compatibly

```java
public record ReportEntry(
        // unchanged — these three drive the CSV and any existing consumer
        @JsonProperty("Client_Information") String clientInformation,
        @JsonProperty("Product_Information") String productInformation,
        @JsonProperty("Total_Transaction_Amount") long netQuantity,
        // dimensions, decoded from ReportKey
        String clientType, String clientNumber, String accountNumber, String subaccountNumber,
        String exchangeCode, String productGroupCode, String symbol, LocalDate expirationDate,
        // measures, from NetPosition
        long grossLong, long grossShort, int tradeCount,
        LocalDate firstTransactionDate, LocalDate lastTransactionDate, Instant lastUpdatedAt,
        Map<String, BigDecimal> feesByCurrency) {}
```

The three original properties keep their exact JSON names, so the change is
additive and the existing frontend would keep working untouched.

Sort order stays `clientInformation` then `productInformation`, preserving the
row order the golden CSV depends on.

### State store migration

Both the key format and the value type change, so any *pre-existing*
`net-quantity-store` data and its changelog are incompatible — Streams would
attempt to deserialize `Long`-serialized values as `NetPosition`.

**The store name stays `net-quantity-store` and `application-id` stays
`processing-service`.** A `-v2` suffix would bake a version into an identifier
that outlives the migration causing it by a long way, and would need a `-v3`
next time. Migration is handled by teardown instead, which is legitimate here
because **no deployment path in this repo persists state**:

| Path | Kafka data | Streams `state.dir` |
|---|---|---|
| `docker compose` | no volumes declared at all — removed by `down` | container filesystem — removed by `down` |
| `k8s` | `emptyDir: {}` in `kafka.yaml` — pod-lifetime only | no `volumeMounts` on processing-service — pod-lifetime only |
| broker-only (`up -d kafka` + Maven) | container — removed by `down` | **on the host**, at `${TMPDIR:-/tmp}/kafka-streams/processing-service` |

So the incompatible-changelog scenario cannot arise in the containerised paths;
there is nothing to orphan. Only the broker-only development loop leaves state
behind, because RocksDB then lives on the host rather than in a container.

`processing-service/README.md` gains a teardown note covering both:

```bash
docker compose down -v                                    # containerised paths
rm -rf "${TMPDIR:-/tmp}/kafka-streams/processing-service"  # broker-only loop, host-side state
```

`${TMPDIR:-/tmp}` is load-bearing: Kafka Streams derives `state.dir` from
`java.io.tmpdir`, which is `/tmp` on Linux but a per-user `/var/folders/.../T/`
on macOS, so a hardcoded `/tmp/kafka-streams` silently no-ops there.

Kafka Streams defaults `auto.offset.reset` to `earliest` (unlike a plain
consumer, which defaults to `latest`) and nothing overrides it in
`application.yml`, so after teardown the source topic replays from the beginning
and the store rebuilds with the new key format automatically.

---

## Slice 2 — Ingestion status endpoint and a narrow proxy route

### `GET /api/v1/ingest/status`

```java
public record IngestionStatus(
        String configuredPath,      // the ingestion.file-path config value, NOT the resolved absolute path
        boolean fileExists,
        Long fileSizeBytes,         // null when absent
        Instant fileLastModified,   // null when absent
        // last actually-performed ingestion; all null when none has run this process lifetime
        Instant lastIngestAt,
        String fingerprint,
        Integer totalLines, Integer published, Integer skipped, Integer errorCount) {}
```

Always returns `200`. "Never ingested" is a normal state expressed as null
run-fields, not an error — the UI renders "not yet ingested".

**Path disclosure.** `IngestionController`'s existing exception handler
deliberately strips the absolute path from error responses (logging it
server-side, returning a generic message) so container filesystem layout is not
advertised. This endpoint keeps faith with that decision by returning the
*configured* value (`sample-data/Input.txt`) rather than the resolved absolute
path. That is the value an operator wants to verify and it leaks nothing.

### Tracking the ingest timestamp

Nothing records when an ingestion happened. `IngestionRegistry` caches
`IngestionResult` by fingerprint but carries no time.

Add an `AtomicReference<LastIngest>` to `IngestionRegistry`, updated **only when
a result is actually computed**, not on a cache hit. A cache hit publishes
nothing to Kafka, so it must not advance `lastIngestAt` — otherwise the panel
would claim fresh activity where none occurred. `forceCompute` does update it.

`errorCount` is `result.errors().size()`; the error list itself is not exposed
(raw lines contain client data).

### Routing: read-only enforced by the route shape

`frontend/nginx.conf.template` gains an **exact-match** location, placed before
the existing `/api/` prefix location:

```nginx
location = /api/v1/ingest/status {
    set $ingestion ${INGESTION_SERVICE_UPSTREAM};
    proxy_pass $ingestion;
    proxy_set_header Host $host;
}
```

Exact match means `POST /api/v1/ingest` does not match this location. It falls
through to `/api/` → processing-service, which has no such route, and 404s. So
"the UI cannot trigger ingestion" is a property of the routing shape rather than
a rule someone can forget to enforce. `proxy_pass` with a variable requires
runtime DNS resolution, which the existing `resolver ${NGINX_LOCAL_RESOLVERS}`
directive already provides.

Wiring for the new `INGESTION_SERVICE_UPSTREAM` variable:

| File | Change |
|---|---|
| `frontend/Dockerfile` | no change needed — it copies the template to `/etc/nginx/templates/` and the nginx image's own entrypoint substitutes *every* environment variable, with no explicit list to extend |
| `docker-compose.yml` | `INGESTION_SERVICE_UPSTREAM: http://ingestion-service:8081` on the `frontend` service |
| `k8s/frontend.yaml` | same as an env var on the container |
| `frontend/proxy.conf.json` | `"/api/v1/ingest": { "target": "http://localhost:8081", "secure": false }` for dev |

In `proxy.conf.json`, the more specific `/api/v1/ingest` key must be declared
before `/api` for dev-server matching.

---

## Slice 3 — Tailwind v4 and validated design tokens

Install `tailwindcss`, `@tailwindcss/postcss`, `postcss`; add
`frontend/.postcssrc.json` with the `@tailwindcss/postcss` plugin. Angular 21's
`@angular/build:application` builder picks up PostCSS config automatically, so
`angular.json` needs no change. `src/styles.css` becomes `@import "tailwindcss";`
plus the token block.

### Colors

Taken from the `dataviz` skill's validated reference palette. **Net quantity is
polarity data** — net long versus net short around a zero baseline — so its
color job is *diverging*, not categorical or sequential: blue ↔ red with a
neutral gray midpoint.

Validated with `scripts/validate_palette.js`, all six checks PASS in both modes:

| Mode | Long | Short | Worst-pair CVD ΔE | Normal-vision ΔE |
|---|---|---|---|---|
| light (surface `#fcfcfb`) | `#2a78d6` | `#e34948` | 21.6 (protan) | 32.3 |
| dark (surface `#1a1a19`) | `#3987e5` | `#e66767` | 19.2 (protan) | 29.0 |

Both clear the ≥8 CVD target and ≥15 normal-vision floor with a wide margin.
Colour is nonetheless never the sole channel: the signed number is always
rendered as text beside the bar, and status badges always pair an icon with a
label.

Neutral/flat midpoint gray `#f0efec` light / `#383835` dark. Chrome and ink
(surfaces, primary/secondary/muted ink, gridline, border) come from the same
reference table. Status colors (`good #0ca30c`, `warning #fab219`,
`critical #d03b3b`) are reserved for connection and expiry state and never reused
as data colors.

### Dark mode is selected, not flipped

Dark values are the palette's own dark steps, not an automatic inversion.
Following the palette's scoping rule so an explicit theme choice wins over the
OS setting in both directions:

```css
:root { --surface-1: #fcfcfb; --net-long: #2a78d6; /* … */ }

@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) { --surface-1: #1a1a19; --net-long: #3987e5; /* … */ }
}
:root[data-theme="dark"] { --surface-1: #1a1a19; --net-long: #3987e5; /* … */ }
```

Semantic properties are exposed to Tailwind utilities via `@theme inline`, which
(unlike plain `@theme`) emits utilities that *reference* the custom properties
rather than inlining their values — required for the values to swap per mode.

A theme toggle cycles light → dark → auto, persisted in `localStorage`, stamping
`data-theme` on `<html>`; auto removes the attribute.

---

## Slice 4 — Table, filters, column picker, auto-refresh

### Column definitions drive header, cells, and picker

One declarative array is the single source of truth for the table. Adding a
column is a one-line change and cannot desynchronise the header from the cells:

```ts
interface ColumnDef {
  id: keyof ReportEntry | string;
  label: string;
  group: 'Client' | 'Product' | 'Position' | 'Activity' | 'Legacy';
  align: 'left' | 'right';
  numeric: boolean;          // drives tabular-nums + right alignment
  defaultVisible: boolean;
  sortValue: (e: ReportRow) => string | number;
  render: 'text' | 'date' | 'number' | 'divergingBar' | 'expiry';
}
```

| Group | Columns | Visible by default |
|---|---|---|
| Client | client number, account number | yes |
| Client | client type, subaccount number | no |
| Product | symbol, expiration date | yes |
| Product | exchange code, product group code | no |
| Position | net quantity, gross long, gross short, trade count | yes |
| Activity | first/last transaction date, last updated | no |
| Legacy | `Client_Information`, `Product_Information` | no |

Eight visible by default, seventeen available. Visibility persists to
`localStorage`; unknown or missing ids in stored preferences fall back to
defaults so a future column-set change cannot leave a user with a broken table.

### Net quantity cell

A diverging inline bar off a centre baseline: blue extending right for net long,
red extending left for net short, neutral gray for flat. Width scales to the
maximum absolute net in the **currently filtered** set. Per the skill's mark
specs: 4px rounded data-end anchored to the baseline, a 2px surface gap between
adjacent fills, `font-variant-numeric: tabular-nums` on the value so columns
align, and the signed value always present as text.

Flat rows (`netQuantity === 0`) are labelled "flat" — and because gross long and
gross short are now available, a flat row that had activity is visibly different
from one that had none, which is the ambiguity this slice exists to remove.

### Expiry is measured against trade date, not wall clock

The sample data expires `2010-09-10`. Measured against today, every row would
carry a red "expired" badge — noise that conveys nothing. Days-to-expiry is
therefore computed as `expirationDate - lastTransactionDate`, which for the
sample gives `2010-09-08 → 2010-09-10` = 2 days: near-expiry, informative, and
correct. Badge thresholds: negative → `critical` "expired"; 0–7 days →
`warning` "N days"; otherwise a plain date with no badge. Icon plus label in
every case.

**The badge must say what it is measured against.** Wording is "expired as of
trade date" / "N days from trade date", not a bare "expired", so it cannot be
misread as live contract status — the reader is looking at historical data and
the badge is a statement about that data, not about today. This is display-only
wording and trivially reversible if the source data ever becomes current.

### Filters

One row above the table: dimension comboboxes for **client**, **account**, and
**product**, each auto-populated from the distinct values present in the current
response (3–4 options today, hundreds later, no code change), plus a global
substring search across all visible columns. Filters compose with AND. A
"showing N of M rows" line and a clear-all control appear whenever any filter is
active; a filtered-to-empty result is a distinct state from a genuinely empty
report.

### Auto-refresh

Default **on** at a 5-second interval, toggled by a switch whose state persists
to `localStorage`. With auto-refresh off, a manual **Refresh** button is enabled.
Either way a "last updated Ns ago" readout is shown.

- Polling **pauses while `document.visibilityState === 'hidden'`** and resumes
  with an immediate fetch, so a backgrounded tab does not poll indefinitely.
- **A failed refresh must not destroy the table.** `ReportService` gains an
  explicit distinction: a failure during *initial* load shows the error screen
  as today; a failure during a *refresh* keeps the last good rows on screen and
  raises a stale badge. Without this, one blip every 5 seconds would blank the
  page.
- The existing 503 → retry-every-3s behaviour for "Kafka Streams still starting"
  is preserved unchanged, including the `retryCount() > 10` stuck notice.
- Rows whose `lastUpdatedAt` advanced since the previous poll flash briefly, so a
  live table looks live. The flash respects `prefers-reduced-motion`.

### KPI row

Stat tiles, per the skill's "a handful of headline numbers → KPI row, not a
grouped bar chart":

| Tile | Value |
|---|---|
| Transactions | `sum(tradeCount)` over filtered rows |
| Client/product pairs | filtered row count |
| Distinct clients | distinct `Client_Information` |
| Records ingested | `published`, from the status endpoint |
| Fees | per-currency totals, e.g. `USD 12,480.50 · JPY 1,203,000` |

Transactions and records-ingested together are a genuine reconciliation: if
`sum(tradeCount)` and `published` disagree, records were dropped between
ingestion and aggregation. When they disagree the tile carries a `warning` badge.

**Deliberately omitted: a "total net quantity" tile.** Summing net quantity
across different contracts adds quantities of different instruments, which is not
a number. The KPI row is kept in full; only this one tile is dropped, and the
tiles above replace it with aggregates that are all genuinely additive.

**Fees are aggregated per currency, never blended.** `NetPosition` carries
`Map<String, BigDecimal> feesByCurrency`, accumulating each of the three fee
amounts (`exchBrokerFee`, `clearingFee`, `commission`) under **its own** currency
code. Summing maps across rows is then always valid, and the tile renders one
figure per currency. This sidesteps the cross-currency problem entirely rather
than papering over it with a `MIXED` marker: there is no point at which two
currencies are added together.

### Component structure

`Report` is currently one component driving one service. Split so each unit has
one purpose and can be tested alone:

| Unit | Responsibility |
|---|---|
| `report.service.ts` | report HTTP, polling, load-vs-refresh error states |
| `ingestion-status.service.ts` | status HTTP |
| `report-columns.ts` | the `ColumnDef` array (no logic) |
| `column-preferences.ts` | visible-column signal + persistence |
| `report-filters.ts` | filter signals + derived filtered/sorted rows |
| `report.ts` | page shell and composition |
| `kpi-row.ts`, `source-file-panel.ts`, `report-table.ts`, `column-picker.ts`, `filter-bar.ts`, `refresh-control.ts`, `theme-toggle.ts` | presentation |

### Layout

Header (title, connection state, theme toggle) → source-file panel → KPI row →
filter bar with column picker and refresh control → table. The table sits in an
`overflow-x: auto` container so a wide column selection scrolls inside its own
region and the page body never scrolls horizontally. The table header is sticky.

---

## Error handling summary

| Condition | Behaviour |
|---|---|
| Streams store not ready (503) | Retry every 3s; stuck notice past 10 attempts (unchanged) |
| Initial report load fails | Error banner + Retry button (unchanged) |
| Refresh fails, data already shown | Keep rows, show stale badge, keep polling |
| Report returns `200 []` | "No transactions recorded yet" — distinct from a filtered-empty table |
| Status endpoint fails | File panel shows unavailable; the report is unaffected |
| File never ingested | Panel shows "not yet ingested"; run-fields null |
| Malformed 8-part key | `ReportKey.decode` throws; surfaces as a server error rather than blank columns |

## Testing

**`common`**
- `ReportKey` encode → decode round-trip; `clientInformation()` /
  `productInformation()` match the concatenation rule in `docs/file-spec.md`;
  `decode` rejects non-8-part input.
- `NetPosition.plus` accumulates net, gross long, gross short, trade count, and
  min/max transaction date; net matches `sum(quantityLong - quantityShort)`.
- `feesByCurrency` keys each of the three fee amounts under its own currency
  code, and a transaction whose fee currencies differ from each other produces
  separate map entries rather than one blended total.

**`ingestion-service`**
- `KafkaKeyBuilder` emits `ReportKey.encode()` output.
- Status endpoint: never-ingested nulls, post-ingest values, missing file
  (`fileExists=false`), and **a cache hit does not advance `lastIngestAt`**.
- Existing tests must still pass — the key format change affects assertions.

**`processing-service`**
- Topology test: aggregation over `TimestampedTransaction`, dedup still drops
  repeated `transactionId`s.
- `ReportEntry` decodes dimensions from the key correctly.
- **Golden CSV test: `GET /api/v1/report/csv` is byte-identical to
  `sample-output/Output.csv`** after ingesting `sample-data/Input.txt`. This is
  the contract lock for "the CSV format does not change." The fixture is **240
  bytes, LF-only**, rows sorted by client then product, with a trailing `\n` —
  matching what the controller's per-line `append('\n')` produces, so
  "byte-identical" is literal.

  The fixture originally committed was 246 bytes with CRLF terminators and rows in
  input-encounter order, and had therefore never matched the API. That went
  unnoticed because the golden test asserted against a hardcoded literal holding
  the API's form rather than reading the file. Values matched all along; only line
  endings and ordering differed. The API was ruled authoritative and the fixture
  regenerated. The test must compare strictly — no line-ending normalisation and
  no re-sorting, or it stops detecting exactly the drift that hid this.
- `FullPipelineGoldenTest` continues to pass end-to-end.
- 503-when-store-not-ready preserved.

**`frontend`**
- Table renders from `ColumnDef` — header and cell counts agree for any
  visibility subset.
- Filter composition (AND across dimensions plus search); filtered-empty state.
- Column preferences persist; unknown stored ids fall back to defaults.
- Auto-refresh with fake timers: interval fires, pauses when hidden, resumes on
  visible, manual button only when off.
- **Refresh failure retains previously loaded rows** (the regression this spec
  exists to prevent).
- Existing `report.spec.ts`, `report.service.spec.ts` and
  `report.integration.spec.ts` updated for the new DTO and template.

**Manual**
- **Start from `docker compose down -v --remove-orphans`**, the same pattern
  `docs/superpowers/plans/2026-08-12-docker-compose-full-stack.md` already uses.
  This is not hygiene — since the migration strategy is
  "teardown rather than rename", starting from a torn-down state is what actually
  *exercises* the migration path instead of assuming it. Verifying against a
  broker that was never wiped would silently skip the one thing this decision
  rests on.
- Then `docker compose up -d --build`, `curl -X POST localhost:8081/api/v1/ingest`,
  open `localhost:8080`: verify columns, filters, both themes, auto-refresh on
  and off, the fee tile's per-currency figures, the "as of trade date" expiry
  wording, and that the downloaded CSV matches `sample-output/Output.csv`.
- Confirm `POST /api/v1/ingest` through the frontend origin returns 404.
- For the broker-only loop, confirm `rm -rf "${TMPDIR:-/tmp}/kafka-streams/processing-service"`
  is required and sufficient: without it, a store built by the pre-change code
  fails to restore; with it, the store rebuilds cleanly.

## Out of scope

- Per-row fee and commission **columns**, and buy/sell and open/close split
  counts — available from `FutureTransaction` and deliberately deferred. Fee
  *totals* are in scope as a per-currency KPI tile, backed by
  `NetPosition.feesByCurrency`; what is deferred is surfacing the three fee types
  as individual sortable columns.
- Server-side filtering or pagination.
- Triggering ingestion from the UI.
- Exposing per-line parse errors (raw lines contain client data).
- Charts. At 5 rows a chart would be decoration; the table plus KPI row is the
  right form.
