# UI fixes and test-data generator — design

Date: 2026-08-13
Status: approved

## Problem

Four defects surfaced while reviewing the redesigned report UI, plus one gap in
how the pipeline gets exercised:

1. The Client filter offers `Client_Information` — the concatenation of client
   type, client number, account and subaccount. Every account of a client is a
   separate option, so there is no way to ask "show me this client".
2. The Account filter is noise: an account number is meaningless without the
   client it belongs to, and the Client filter already pins one.
3. The Source file panel shows "Not yet ingested." forever, even as the table
   fills with rows.
4. The Fees KPI is dead weight. All sample records carry `0` for the JPY fees,
   so the tile reports a number nobody uses.
5. Only one input file exists. Nothing exercises parse failures, an empty file,
   a missing file, or a record count large enough to be interesting.

## Scope

Frontend changes plus a data generator and a one-line compose change. **No Java
changes.** The CSV contract (`Client_Information`, `Product_Information`,
`Total_Transaction_Amount`) is untouched, and so is fee parsing in `common` and
`processing-service`.

## 1. Filters

Both concatenated dimensions are decomposed into the fields they were built
from. `Client_Information` is `clientType + clientNumber + accountNumber +
subaccountNumber`; `Product_Information` is `exchangeCode + productGroupCode +
symbol + expirationDate`.

| Filter | Before | After |
|---|---|---|
| Client | `entry.Client_Information` | `entry.clientNumber`, labelled **Client number** |
| Account | `entry.accountNumber` | removed |
| Product | `entry.Product_Information` | `entry.symbol`, labelled **Symbol** |
| Exchange | — | `entry.exchangeCode`, new |
| Product group | — | `entry.productGroupCode`, labelled **Group**, new |
| Expiry | — | `entry.expirationDate`, new |

The bar becomes: Client number, Exchange, Group, Symbol, Expiry, Search. Each
dropdown is one independent dimension, so any combination of them is expressible
— which is the point of decomposing. Selecting a client number spans all of that
client's accounts and subaccounts; selecting an exchange spans every product
traded there; selecting an expiry spans every product with that contract month.

Every component of `Product_Information` gets a filter, with no exceptions to
explain. The alternative — filtering only the components that vary in the sample
file — was rejected: cardinality is a property of the file loaded, not of the
format, and a rule derived from one file would be wrong for the next one.

Removing the Account filter removes: the dropdown in `filter-bar.ts`, the
`account` signal, `accountOptions`, `setAccount`, and the `account` entries in
`FilterCriteria`, `activeFilterCount` and `clearAll`. Each new filter adds the
mirror-image set.

Nothing becomes unreachable. The free-text Search still scans the `sortValue` of
every column, so account number and subaccount stay findable by typing.

### What the sample data looks like through these filters

Measured over all 717 sample records:

| Component | Distinct values |
|---|---|
| `clientNumber` | 2 — `1234`, `4321` |
| `exchangeCode` | 2 — `CME` (511), `SGX` (206) |
| `productGroupCode` | 1 — `FU` on every record |
| `symbol` | 3 — `N1` (214), `NK.` (297), `NK` (206) |
| `expirationDate` | 1 — `20100910` on every record |

Two consequences worth stating up front, so neither reads as a bug:

- **Group and Expiry will show a single option** with this input. That is the
  file, not the control. `large-7000.txt` varies both (section 5), so the
  dropdowns can be seen doing real work.
- **Exchange and symbol are independent.** The combinations present are
  `CME|N1`, `CME|NK.` and `SGX|NK`, so selecting `CME` yields 511 records across
  two symbols — a view no single symbol selection can produce. This is also why
  `NK` (SGX) and `NK.` (CME), near-indistinguishable in a dropdown, are
  separable at all.

### Rendering the Expiry options

`expirationDate` arrives as an ISO date string (`"2010-09-10"`), and the table
renders it through a bespoke `expiry` template. The dropdown displays the same
formatted text but carries the raw ISO string as the option `value`, so the
comparison in `filterAndSort` stays an exact string match against the entry
field. Formatting must not leak into the filter value.

### Rejected: keep filtering on the concatenated strings

The concatenation (`SGXFUNK20100910`) is at least a fully-specified contract, so
this was a real option for the Product filter. Rejected because it collapses four
independent dimensions into one: with concatenated values there is no selection
that means "everything on CME", or "every product expiring in September".

## 2. Default columns

Two edits in `report-columns.ts`.

**Reorder** the `Client` group so declaration order — which is also the render
order, enforced by `ColumnPreferences.visibleColumns` — reads client type,
client number, account, subaccount.

**Re-flag** `defaultVisible`:

| Column | Before | After |
|---|---|---|
| `clientType` | `false` | `true` |
| `subaccountNumber` | `false` | `true` |
| `expirationDate` | `true` | `false` |

The default row becomes: Client type, Client, Account, Subaccount, Symbol, Net,
Gross long, Gross short, Trades. Expiry remains in the column picker under
Product, and now also has its own filter (section 1); nothing is deleted.

### localStorage migration

`ColumnPreferences` persists the visible set under `pfm.visibleColumns`, and
`restore()` prefers any stored array over the defaults. Without intervention,
anyone who has already opened the UI keeps Expiry and never sees Client type or
Subaccount — the change would be invisible to exactly the people reviewing it.

The key becomes `pfm.visibleColumns.v2`. That is a one-time reset to the new
defaults; the old key is left to expire on its own rather than being migrated,
since the stored value is a UI preference with no user-authored content in it.

## 3. Ingestion status frozen at "Not yet ingested."

### Root cause

`Report.ngOnInit()` calls `statusService.load()` exactly once. Nothing else ever
requests `/api/v1/ingest/status`.

`ReportService`, by contrast, polls `/api/v1/report` every 5 seconds. Ingestion
is triggered out of band — `POST :8081/api/v1/ingest`, deliberately not routable
from the UI (see the exact-match `location` rule in `nginx.conf.template`). So
the ordinary sequence is: open the UI, panel renders `lastIngestAt === null` →
"Not yet ingested.", run the curl, the table fills in from the poll, and the
panel keeps asserting nothing has been ingested until the page is hard-reloaded.

### Fix

Reload the status on the same beat as the report. `ReportService` already
exposes `lastLoadedAt`, a signal set on every successful fetch — initial load,
poll tick, and manual Refresh alike. An `effect()` in `Report` calls
`statusService.load()` whenever it changes.

This keeps `ReportService` unaware of `IngestionStatusService` (the dependency
would otherwise run from the lower-level service to the higher-level one), and
it inherits the auto-refresh toggle and the visibility-change pause for free —
when polling stops, status stops refreshing with it, which is correct.

The cost is one extra GET per 5s poll. The handler stats a single file and reads
an in-memory `AtomicReference`.

### Rejected alternatives

- **A second polling timer inside `IngestionStatusService`.** Duplicates the
  interval, visibility and auto-refresh logic already in `ReportService`, and
  lets the two panels drift out of sync with each other.
- **Refresh the status only when the row count changes.** Misses the case that
  matters most: a re-ingest of the same file changes `lastIngestAt`, `published`
  and `errorCount` while the aggregate totals stay identical.

### Test

A spec asserting that a second report fetch triggers a second
`/api/v1/ingest/status` request.

## 4. KPI row

Remove the Fees tile from `kpi-row.ts` along with its `feeEntries` computed and
the spec coverage that pins it. Add **Distinct products**, counting distinct
`Product_Information` over the filtered rows — the counterpart to Distinct
clients, and it keeps the grid at four tiles.

`feesByCurrency` stays in the API response and fees stay parsed. The tile is
uninformative for this data set; the field is not wrong, and removing it would
reach into `NetPosition`, the Kafka state stores and the golden test for no
benefit.

The reconciliation warning on the Transactions tile is unaffected.

## 5. Test-data generator

`scripts/gen-test-data.py` writes fixtures into `sample-data/generated/`, which
sits inside the read-only mount compose already gives `ingestion-service`. A
`sample-data/generated/` entry is added to `.gitignore`: the fixtures are
reproducible from the script, and `large-7000.txt` alone is roughly 1.3 MB of
generated data that would otherwise live in git history forever.

Generated files are derived from `sample-data/Input.txt` rather than built from
scratch: the generator reads real lines as templates and patches fixed slices —
client number at offset 8–11, product group at 26–27, exchange at 28–31, symbol
at 32–37, expiration date at 38–45, quantities at 53–62 and 64–73 (1-based, per
`FieldPositions`). Every line it calls valid is therefore
layout-correct by construction, without re-implementing the 176-byte spec in a
second language where it could drift.

| File | Exercises |
|---|---|
| `large-7000.txt` | 7000 valid records; client number, exchange, product group, symbol and expiry all varied, so every filter dropdown has several options and the row count and KPIs are non-trivial |
| `mixed-errors.txt` | mostly valid, a handful of bad lines — `errorCount > 0`, Failed renders red, report still populates |
| `truncated-line.txt` | line shorter than 176 bytes |
| `bad-quantity.txt` | non-numeric quantity field |
| `bad-date.txt` | expiration date `20101332` |
| `blank-lines.txt` | blank and whitespace-only lines interleaved |
| `all-invalid.txt` | zero parseable records — `published: 0`, empty report, no crash |
| `empty.txt` | zero-length file |

The missing-file path (404 `IngestionFileNotFoundException`) needs no fixture:
point `INGESTION_FILE_PATH` at a path that does not exist.

`all-invalid.txt` is worth stating precisely, because it sits next to a throw:
`IngestionService.runIngestion` raises `KafkaPublishException` when
`published == 0` **and** `parseResult.records()` is non-empty. With every line
unparseable the record list is empty, so the guard does not fire and the run
reports `published: 0, skipped: <totalLines>` cleanly.

### Switching files

`docker-compose.yml` changes one line:

```yaml
INGESTION_FILE_PATH: ${INGESTION_FILE_PATH:-/app/sample-data/Input.txt}
```

Default behaviour is unchanged. To use a fixture:

```bash
python3 scripts/gen-test-data.py
docker compose down -v
INGESTION_FILE_PATH=/app/sample-data/generated/large-7000.txt docker compose up -d
```

`down -v` is not optional. The report is a running aggregate keyed by
`sha256(contentHash + ":" + lineNumber)`, so a different file adds to the
existing totals instead of replacing them. Skipping the teardown produces
numbers that look wrong and are not.

Usage goes in a short README section alongside the existing run instructions.

## Out of scope

- Any Java change.
- Removing fees from the domain model or the API.
- Making ingestion triggerable from the UI. The routing rule that prevents it is
  a deliberate control, documented as an invariant.
