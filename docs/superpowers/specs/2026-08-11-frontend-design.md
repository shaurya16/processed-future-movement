# Design: `frontend` — Angular report viewer + CSV download

Status: Approved
Branch: `feature/frontend`

## Context

Fourth buildable slice of processed-future-movement, following `common`,
`ingestion-service`, and `processing-service` (see
[2026-08-09-processing-service-design.md](2026-08-09-processing-service-design.md)). Per
the chosen architecture (see [README.md](../../../README.md)):

```
Input.txt --> ingestion-service --> Kafka topic --> processing-service --> REST API --> frontend
                                                          (Kafka Streams          (Angular)
                                                           aggregation)
```

`frontend` is an Angular app that calls `processing-service`'s REST API
(`GET /api/report`, `GET /api/report/csv`, both on `localhost:8082`) to display the
daily summary table and let the user download it as `Output.csv`.

**The open problem this design exists to solve:** `processing-service` returns `503`
(not an empty `200`) while its Kafka Streams state store isn't ready yet — see decision
10 in the processing-service design. A naive UI that treats "request failed" and
"request succeeded with zero rows" the same way would show an empty table in both
cases, hiding the fact that the aggregate simply hasn't started yet from a case where
it's genuinely computed zero data so far. This design's job is to keep those two states
visually and behaviorally distinct.

## Decisions made during brainstorming

1. **Not-ready handling — auto-poll until ready, no retry cap.** On `503`, the UI stays
   in a `loading` state and retries `GET /api/report` on a fixed timer until it
   succeeds, rather than requiring the user to click a manual retry. No cap on retry
   count: the store is expected to become ready within moments of Kafka Streams
   starting, and an unbounded retry loop is simpler than reasoning about a
   give-up threshold that would just be another way to show a false "broken" state.
2. **Dev wiring — Angular dev-server proxy, not backend CORS.** `processing-service` has
   no CORS configuration today. Rather than add one, the Angular CLI dev server proxies
   `/api/*` to `localhost:8082` (`proxy.conf.json`), so the app calls relative paths.
   This also anticipates the later k8s slice, where frontend and processing-service will
   likely sit behind a shared origin/reverse proxy rather than needing CORS in
   production either.
   - **Rejected: `@CrossOrigin`/`CorsConfiguration` on `processing-service`.** Works
     for both dev and prod, but adds backend configuration purely to serve a dev-time
     need, and doesn't match how the stack will likely be deployed (same-origin via
     k8s ingress).
3. **CSV download — plain anchor tag, not a JS-driven blob fetch.** `<a
   href="/api/report/csv" download>` lets the browser handle the download natively
   using `processing-service`'s existing `Content-Disposition: attachment` header. No
   extra JS, no loading/error state to manage for the download click itself.
   - **Rejected: `HttpClient` fetch + Blob + programmatic save.** Would allow showing a
     spinner/error during the download click, but that's unneeded complexity for a
     same-origin, already-content-disposed endpoint — the browser's native download
     handling is sufficient.
4. **Refresh — manual button, not continuous polling after first success.** Once the
   report loads successfully (`200`), polling stops. A **Refresh** button lets the user
   explicitly re-fetch to see updated aggregates as more transactions are ingested.
   - **Rejected: continuous auto-refresh on an interval.** Would keep the table live
     without user action, but risks the displayed rows changing while a user is reading
     them, and isn't needed for this slice's scope.
   - **Note:** clicking Refresh re-runs `load()`, which sets `status` back to
     `loading` first. If the store happens to have become momentarily unready again
     (unlikely in practice once initially `RUNNING`), the loading banner reappears
     until the retry succeeds — same code path as the initial load, no special case.
5. **State management — signals in an injectable service, not RxJS + async pipe.** A
   `ReportService` exposes plain signals (`status`, `entries`, `errorMessage`) that the
   component reads directly in its template. Chosen for straightforward reasoning about
   a simple state machine (`loading` / `ready` / `error`) without RxJS operator
   composition the app doesn't otherwise need.
   - **Rejected: `Observable<ReportState>` + `async` pipe.** More idiomatic for complex
     stream composition, but this app has no such complexity — a single polling loop
     writing to signals is simpler to read and test.
6. **No UI component library — hand-written CSS.** Matches this project's
   minimal-dependency pattern already established in the backend services (e.g. manual
   CSV string-building instead of a CSV library in `processing-service`). Angular
   Material was considered and rejected as unnecessary weight for a single-page
   internal tool with one table, one banner, and two buttons.
7. **Wire format — `ReportEntry` mirrors JSON keys exactly, no camelCase translation.**
   `processing-service` already emits `Client_Information`, `Product_Information`,
   `Total_Transaction_Amount` as the literal JSON property names (processing-service
   design decision 8). The frontend's `ReportEntry` interface uses those same key names
   directly — no mapping layer.

## Components

**`ReportEntry`** (`report/report-entry.ts`) — interface matching the wire format:
```ts
interface ReportEntry {
  Client_Information: string;
  Product_Information: string;
  Total_Transaction_Amount: number;
}
```

**`ReportService`** (`report/report.service.ts`, `providedIn: 'root'`) — owns report
state and the polling loop:
- `status: Signal<'loading' | 'ready' | 'error'>`
- `entries: Signal<ReportEntry[]>` — meaningful only when `status === 'ready'`; may be
  `[]` (genuinely zero rows), which is a valid `ready` state, not `loading`.
- `errorMessage: Signal<string | null>` — meaningful only when `status === 'error'`.
- `load()` — sets `status = 'loading'`, issues `GET /api/report` via `HttpClient`:
  - `200` → `status = 'ready'`, `entries` set from the response body (including `[]`).
  - `503` → stays `status = 'loading'`; schedules another `load()` after a fixed delay
    (3000ms, via `timer(3000)`) and repeats until success.
  - any other error (network failure, non-503 HTTP error) → `status = 'error'`,
    `errorMessage` set from the error; polling stops.

**`ReportComponent`** (`report/report.component.ts/html/css`) — the app's only feature
component. Calls `reportService.load()` on init. Renders based on `status`:
- `loading` → banner: "Report is still being generated — Kafka Streams is starting up.
  This can take a few moments."
- `error` → banner with `errorMessage` + a **Retry** button (calls `load()`).
- `ready` + `entries().length === 0` → "No transactions recorded yet." (distinct
  copy from the loading banner — this is the "genuinely zero rows" case).
- `ready` + `entries().length > 0` → table with columns `Client_Information` /
  `Product_Information` / `Total_Transaction_Amount`, a **Download CSV** link
  (`<a href="/api/report/csv" download>`), and a **Refresh** button (calls `load()`).

**`AppComponent`** (`app.component.ts/html/css`) — root shell, hosts `ReportComponent`.

**`proxy.conf.json`** — dev-server config forwarding `/api/*` to
`http://localhost:8082`, wired into `angular.json`'s `serve` target.

## Data flow

1. App bootstraps, `ReportComponent` calls `reportService.load()`.
2. `ReportService` sets `status = 'loading'` and issues `GET /api/report` (proxied to
   `processing-service:8082` in dev).
3. While `processing-service`'s Kafka Streams app isn't `RUNNING`, each request returns
   `503`; `ReportService` stays in `loading` and retries every 3s.
4. Once `RUNNING`, the request returns `200` with a (possibly empty) JSON array;
   `ReportService` transitions to `ready` and stores `entries`.
5. `ReportComponent` re-renders: empty-state message if `entries` is `[]`, otherwise
   the table, Download CSV link, and Refresh button.
6. Clicking **Download CSV** navigates the browser to `/api/report/csv`; the browser
   downloads `Output.csv` directly using `processing-service`'s response headers — no
   Angular involvement beyond the anchor tag.
7. Clicking **Refresh** re-runs step 2 onward.

## Error handling

- **503 (not ready)** — treated as an expected, transient state, not an error. No error
  message shown; the loading banner persists and polling continues indefinitely.
- **Network failure / non-503 HTTP error** — treated as a genuine error: polling stops,
  `errorMessage` is shown, and the user must click **Retry** to try again. This
  prevents an unbounded retry loop against a backend that's actually broken (as opposed
  to merely still starting up).
- **Empty successful response (`200` with `[]`)** — explicitly not an error and not
  loading; rendered as its own empty-state message so it's never confused with "still
  waiting for data."
- **CSV download failures** (e.g., if `processing-service` is down at click time) are
  handled by the browser's native download-failure UI, not the Angular app — consistent
  with the plain-anchor-tag decision.

## Testing

Unit tests only (Karma/Jasmine, Angular CLI default, `HttpTestingController`):

- **`ReportService`**:
  - `503` → `503` → `200` sequence ends in `status = 'ready'` with the correct
    `entries`, verified with `fakeAsync`/`tick` advancing past the retry timer.
  - `200` with `[]` yields `status = 'ready'` with `entries() = []` — explicitly
    asserted as *not* `loading`.
  - Network/500 error yields `status = 'error'` with a non-empty `errorMessage`, and
    confirms no further retries are scheduled.
- **`ReportComponent`**:
  - Renders the loading banner when `status = 'loading'`.
  - Renders the table with correct rows when `status = 'ready'` and `entries` is
    non-empty.
  - Renders the empty-state message (not the loading banner) when `status = 'ready'`
    and `entries` is `[]`.
  - Renders the error banner and a Retry button when `status = 'error'`; clicking Retry
    calls `reportService.load()`.
  - Renders a Download CSV link with `href="/api/report/csv"` when `status = 'ready'`
    and `entries` is non-empty.

## Out of scope for this slice

- k8s manifests / reverse-proxy configuration for production same-origin serving
  (later slice, though decision 2 anticipates it).
- CORS configuration on `processing-service` (deliberately avoided per decision 2).
- Continuous/live auto-refresh after initial load (deliberately deferred per decision 4).
- Sorting, filtering, or pagination of the report table.
- Any authentication/authorization on the frontend or the API it calls.
