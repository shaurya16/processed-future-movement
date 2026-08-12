# UI Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the three-column report UI with a Tailwind-styled table offering
per-field columns, filters, and auto-refresh — which requires first making the
Kafka Streams grouping key non-lossy so those fields exist at all.

**Architecture:** The fixed-width parser trims each field before concatenating
them into `Client_Information` / `Product_Information`, so sub-field boundaries
are variable-width and unrecoverable in the browser. A `ReportKey` record in
`common` becomes the single source of truth for the key format, making the two
CSV columns *derived* rather than duplicated. The `Long` aggregate value becomes
a `NetPosition` record carrying gross long/short, trade count, dates and
per-currency fees. The frontend then renders a column-definition-driven table.

**Tech Stack:** Java 21 records, Kafka Streams, Spring Boot 3, Angular 21
(signals, standalone components), Tailwind CSS v4, Vitest, JUnit 5.

**Spec:** `docs/superpowers/specs/2026-08-12-ui-tailwind-redesign-design.md`

## Global Constraints

- **CSV output must not change.** `GET /api/v1/report/csv` stays byte-identical
  to `sample-output/Output.csv`: **240 bytes, LF-only**, header
  `Client_Information,Product_Information,Total_Transaction_Amount`, one row per
  entry sorted by client then product, trailing `\n`. Task 7 locks this with a
  golden test.

  Corrected during execution: the committed fixture was originally 246 bytes with
  CRLF terminators and rows in input-encounter order, so it had never matched the
  API — the drift was hidden because the golden test compared against a hardcoded
  literal that encoded the API's form instead of the file's. The values were
  identical throughout; only line endings and row order differed. Ruling: the API
  is authoritative, and the fixture was regenerated to match it.
- **The three original JSON property names are frozen:** `Client_Information`,
  `Product_Information`, `Total_Transaction_Amount`. New fields are additive only.
- **Store name stays `net-quantity-store`; `application-id` stays
  `processing-service`.** No `-v2` suffixes. Migration is by teardown (Task 7).
- **`D` = debit = negative** (`FutureTransactionFactory:77`). Every money field in
  the sample carries `D`, so fee totals are legitimately **negative**
  (e.g. `USD -0.90`). This is correct, not a bug to fix.
- **Path disclosure:** the ingest status endpoint returns the *configured*
  `ingestion.file-path` value, never `path.toAbsolutePath()`.
  `IngestionController` deliberately strips absolute paths from error responses;
  do not regress that.
- **Sort order stays** `clientInformation` then `productInformation` — the golden
  CSV depends on it.
- Row order and `netQuantity` values must be unchanged by this work:
  `sum(quantityLong - quantityShort)` per (client, product).
- Every module builds with `mvn -q test` from the repo root; frontend with
  `npm test` in `frontend/`.

---

## File Structure

**`common`** — shared domain, no Spring, no Jackson dependency.

| File | Responsibility |
|---|---|
| Create `common/src/main/java/com/pfm/common/domain/ReportKey.java` | The 8-field grouping key: `from`/`encode`/`decode`, plus derived `clientInformation()` / `productInformation()`. Single source of truth for the key format. |
| Create `common/src/main/java/com/pfm/common/domain/NetPosition.java` | The aggregate value: net/gross/count/dates/`feesByCurrency`, with `empty()` and `plus()`. |
| Create `common/src/test/java/com/pfm/common/domain/ReportKeyTest.java` | Round-trip, derived values, malformed-input rejection. |
| Create `common/src/test/java/com/pfm/common/domain/NetPositionTest.java` | Accumulation arithmetic incl. per-currency fees. |
| Create `common/src/test/java/com/pfm/common/domain/TestTransactions.java` | Test helper building `FutureTransaction` instances without byte-offset arithmetic. |

**`ingestion-service`**

| File | Responsibility |
|---|---|
| Modify `.../kafka/KafkaKeyBuilder.java` | Delegate to `ReportKey.from(tx).encode()`. |
| Modify `.../IngestionRegistry.java` | Record `lastIngest` (only on actual compute); add a `Clock` constructor for tests alongside the existing no-arg one. |
| Create `.../IngestionStatus.java` | Status response record. |
| Create `.../IngestionStatusService.java` | Reads file metadata + registry's last ingest. |
| Modify `.../IngestionController.java` | Add `GET /ingest/status`. |

**`processing-service`**

| File | Responsibility |
|---|---|
| Create `.../streams/NetPositionSerde.java` | JSON serde for the new aggregate value. |
| Modify `.../streams/AggregationTopology.java` | Aggregate into `NetPosition`; take a `Clock`. |
| Modify `.../report/ReportEntry.java` | Expand to 18 fields; add `of(ReportKey, NetPosition)`. |
| Modify `.../report/ReportService.java` | Read `NetPosition`; decode via `ReportKey`. |
| Modify `processing-service/README.md` | Teardown note for the store migration. |

**`frontend`** — one responsibility per file; `report.ts` becomes a thin shell.

| File | Responsibility |
|---|---|
| Modify `src/styles.css` | Tailwind import + validated palette tokens, light/dark. |
| Create `.postcssrc.json` | Tailwind v4 PostCSS plugin. |
| Create `src/app/shared/local-preference.ts` | `readPreference` / `writePreference` helpers. |
| Create `src/app/shared/theme.ts` | Theme signal + `data-theme` stamping. |
| Create `src/app/shared/theme-toggle.ts` | light → dark → auto control. |
| Modify `src/app/report/report-entry.ts` | Expanded `ReportEntry`; add `IngestionStatus`. |
| Modify `src/app/report/report.service.ts` | Load-vs-refresh states, `stale`, polling. |
| Create `src/app/report/ingestion-status.service.ts` | Status fetch. |
| Create `src/app/report/report-columns.ts` | `ColumnDef[]` — data only, no logic. |
| Create `src/app/report/column-preferences.ts` | Visible-column signal + persistence. |
| Create `src/app/report/report-filters.ts` | Filter signals + derived filtered/sorted rows. |
| Create `src/app/report/report-table.ts` + `.html` | The table, diverging bar, expiry badge. |
| Create `src/app/report/column-picker.ts` | Column show/hide dropdown. |
| Create `src/app/report/filter-bar.ts` | Dimension combos + search. |
| Create `src/app/report/refresh-control.ts` | Auto-refresh switch + manual button. |
| Create `src/app/report/kpi-row.ts` | Stat tiles incl. per-currency fees. |
| Create `src/app/report/source-file-panel.ts` | File provenance panel. |
| Modify `src/app/report/report.ts` + `.html` + `.css` | Shell/composition only. |

**Config** — `frontend/nginx.conf.template`, `frontend/proxy.conf.json`,
`docker-compose.yml`, `k8s/frontend.yaml`. (`frontend/Dockerfile` needs no change:
the nginx image's template entrypoint substitutes every environment variable.)

---

## Task 1: `ReportKey` — the non-lossy grouping key

**Files:**
- Create: `common/src/main/java/com/pfm/common/domain/ReportKey.java`
- Test: `common/src/test/java/com/pfm/common/domain/ReportKeyTest.java`

**Interfaces:**
- Consumes: `FutureTransaction` (existing record in the same package).
- Produces: `ReportKey.from(FutureTransaction) -> ReportKey`,
  `encode() -> String`, `ReportKey.decode(String) -> ReportKey`,
  `clientInformation() -> String`, `productInformation() -> String`, and the
  eight accessors `clientType()`, `clientNumber()`, `accountNumber()`,
  `subaccountNumber()`, `exchangeCode()`, `productGroupCode()`, `symbol()`,
  `expirationDate() -> LocalDate`. Tasks 3, 5 and 6 depend on these exact names.

Background: `LINE_1` below is the first record of `sample-data/Input.txt`. Its
expected derived values are confirmed against the first data row of
`sample-output/Output.csv` (`CL432100020001,SGXFUNK20100910,46`).

- [ ] **Step 1: Write the failing test**

Create `common/src/test/java/com/pfm/common/domain/ReportKeyTest.java`:

```java
package com.pfm.common.domain;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReportKeyTest {

    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    private final FutureTransactionParser parser = new FutureTransactionParser();

    private ReportKey keyOfLine1() {
        return ReportKey.from(parser.parse(LINE_1, 1));
    }

    @Test
    void fromExtractsTheEightGroupingFields() {
        ReportKey key = keyOfLine1();

        assertEquals("CL", key.clientType());
        assertEquals("4321", key.clientNumber());
        assertEquals("0002", key.accountNumber());
        assertEquals("0001", key.subaccountNumber());
        assertEquals("SGX", key.exchangeCode());
        assertEquals("FU", key.productGroupCode());
        assertEquals("NK", key.symbol());
        assertEquals(LocalDate.of(2010, 9, 10), key.expirationDate());
    }

    @Test
    void encodeJoinsAllEightFieldsWithPipes() {
        assertEquals("CL|4321|0002|0001|SGX|FU|NK|20100910", keyOfLine1().encode());
    }

    @Test
    void decodeReversesEncode() {
        ReportKey original = keyOfLine1();

        assertEquals(original, ReportKey.decode(original.encode()));
    }

    @Test
    void derivedInformationFieldsMatchTheReportSpec() {
        ReportKey key = keyOfLine1();

        // These two are the CSV column values; they must be byte-identical to
        // the concatenation rule in docs/file-spec.md.
        assertEquals("CL432100020001", key.clientInformation());
        assertEquals("SGXFUNK20100910", key.productInformation());
    }

    @Test
    void derivedProductInformationHandlesAVariableLengthSymbol() {
        // 'NK.' is 3 chars where line 1's symbol is 2 — the ambiguity that makes
        // client-side splitting impossible, and why the key carries all 8 fields.
        ReportKey key = new ReportKey("CL", "1234", "0003", "0001",
                "CME", "FU", "NK.", LocalDate.of(2010, 9, 10));

        assertEquals("CMEFUNK.20100910", key.productInformation());
        assertEquals("CL|1234|0003|0001|CME|FU|NK.|20100910", key.encode());
    }

    @Test
    void decodeRejectsAKeyThatIsNotEightParts() {
        // The pre-change 2-part format must fail loudly rather than yield blanks.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ReportKey.decode("CL432100020001|SGXFUNK20100910"));

        assertEquals("Expected 8 pipe-delimited parts in report key, got 2: "
                + "CL432100020001|SGXFUNK20100910", e.getMessage());
    }

    @Test
    void decodePreservesTrailingEmptyFields() {
        // split() must use limit -1 or a blank trailing symbol silently drops a part.
        ReportKey key = ReportKey.decode("CL|4321|0002|0001|SGX|FU||20100910");

        assertEquals("", key.symbol());
        assertEquals(LocalDate.of(2010, 9, 10), key.expirationDate());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl common test -Dtest=ReportKeyTest`
Expected: FAIL — compilation error, `cannot find symbol: class ReportKey`.

- [ ] **Step 3: Write the implementation**

Create `common/src/main/java/com/pfm/common/domain/ReportKey.java`:

```java
package com.pfm.common.domain;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * The (client, product) pair a report row aggregates over, carrying all eight
 * underlying fields rather than the two concatenated strings.
 *
 * <p>This type is the single source of truth for the Kafka message key format:
 * {@code ingestion-service} writes it via {@link #encode()} and
 * {@code processing-service} reads it back via {@link #decode(String)}. The two
 * report columns are <em>derived</em> ({@link #clientInformation()},
 * {@link #productInformation()}) rather than stored, because the fixed-width
 * parser trims each field before concatenation — making the sub-field boundaries
 * variable-width and impossible to recover from the joined string alone.
 */
public record ReportKey(
        String clientType,
        String clientNumber,
        String accountNumber,
        String subaccountNumber,
        String exchangeCode,
        String productGroupCode,
        String symbol,
        LocalDate expirationDate) {

    private static final String DELIMITER = "|";
    private static final int FIELD_COUNT = 8;

    public static ReportKey from(FutureTransaction transaction) {
        return new ReportKey(
                transaction.clientType(),
                transaction.clientNumber(),
                transaction.accountNumber(),
                transaction.subaccountNumber(),
                transaction.exchangeCode(),
                transaction.productGroupCode(),
                transaction.symbol(),
                transaction.expirationDate());
    }

    public String encode() {
        return String.join(DELIMITER,
                clientType, clientNumber, accountNumber, subaccountNumber,
                exchangeCode, productGroupCode, symbol,
                DateTimeFormatter.BASIC_ISO_DATE.format(expirationDate));
    }

    public static ReportKey decode(String encoded) {
        // Limit -1 keeps trailing empty fields; the default limit would drop them.
        String[] parts = encoded.split("\\" + DELIMITER, -1);
        if (parts.length != FIELD_COUNT) {
            throw new IllegalArgumentException("Expected " + FIELD_COUNT
                    + " pipe-delimited parts in report key, got " + parts.length + ": " + encoded);
        }
        return new ReportKey(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6],
                LocalDate.parse(parts[7], DateTimeFormatter.BASIC_ISO_DATE));
    }

    /** CLIENT TYPE + CLIENT NUMBER + ACCOUNT NUMBER + SUBACCOUNT NUMBER. */
    public String clientInformation() {
        return clientType + clientNumber + accountNumber + subaccountNumber;
    }

    /** EXCHANGE CODE + PRODUCT GROUP CODE + SYMBOL + EXPIRATION DATE. */
    public String productInformation() {
        return exchangeCode + productGroupCode + symbol
                + DateTimeFormatter.BASIC_ISO_DATE.format(expirationDate);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl common test -Dtest=ReportKeyTest`
Expected: PASS — 7 tests.

- [ ] **Step 6: Commit**

```bash
git add common/src/main/java/com/pfm/common/domain/ReportKey.java \
        common/src/test/java/com/pfm/common/domain/ReportKeyTest.java
git commit -m "feat(common): add ReportKey as the single source of truth for the grouping key"
```

---

## Task 2: `NetPosition` — the enriched aggregate value

**Files:**
- Create: `common/src/main/java/com/pfm/common/domain/NetPosition.java`
- Create: `common/src/test/java/com/pfm/common/domain/TestTransactions.java`
- Test: `common/src/test/java/com/pfm/common/domain/NetPositionTest.java`

**Interfaces:**
- Consumes: `FutureTransaction`.
- Produces: `NetPosition.empty() -> NetPosition`,
  `plus(FutureTransaction, Instant) -> NetPosition`, and accessors
  `netQuantity() -> long`, `grossLong() -> long`, `grossShort() -> long`,
  `tradeCount() -> int`, `firstTransactionDate() -> LocalDate`,
  `lastTransactionDate() -> LocalDate`, `lastUpdatedAt() -> Instant`,
  `feesByCurrency() -> Map<String, BigDecimal>`. Tasks 4, 5 and 6 depend on these.
- Also produces the test helper `TestTransactions.transaction(...)` used by later
  backend tests.

Note: `plus` is immutable — it returns a new instance. `empty()` has null dates
and an empty fee map. Fee amounts are accumulated **only** when the currency is
non-blank and the amount is non-zero, which keeps `{JPY: 0.00}` out of the map
(commission is `0.00` throughout the sample data).

- [ ] **Step 1: Write the test helper**

Create `common/src/test/java/com/pfm/common/domain/TestTransactions.java`:

```java
package com.pfm.common.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/**
 * Builds {@link FutureTransaction} instances for tests without fixed-width byte
 * arithmetic. Only the fields the aggregate reads are parameterised; everything
 * else gets a harmless constant.
 */
final class TestTransactions {

    private TestTransactions() {
    }

    static FutureTransaction transaction(long quantityLong, long quantityShort, LocalDate transactionDate) {
        return transaction(quantityLong, quantityShort, transactionDate,
                BigDecimal.ZERO, "USD", BigDecimal.ZERO, "USD", BigDecimal.ZERO, "JPY");
    }

    static FutureTransaction transaction(long quantityLong, long quantityShort, LocalDate transactionDate,
                                         BigDecimal exchBrokerFee, String exchBrokerFeeCurrency,
                                         BigDecimal clearingFee, String clearingFeeCurrency,
                                         BigDecimal commission, String commissionCurrency) {
        return new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B',
                quantityLong, quantityShort,
                exchBrokerFee, exchBrokerFeeCurrency, 'D',
                clearingFee, clearingFeeCurrency, 'D',
                commission, commissionCurrency, 'D',
                transactionDate, "001238", "0", "688032",
                new BigDecimal("9250.0000000"), "", "", 'O');
    }
}
```

- [ ] **Step 2: Write the failing test**

Create `common/src/test/java/com/pfm/common/domain/NetPositionTest.java`:

```java
package com.pfm.common.domain;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetPositionTest {

    private static final Instant T1 = Instant.parse("2026-08-12T14:30:00Z");
    private static final Instant T2 = Instant.parse("2026-08-12T14:31:00Z");
    private static final LocalDate DAY_1 = LocalDate.of(2010, 8, 19);
    private static final LocalDate DAY_2 = LocalDate.of(2010, 8, 20);

    @Test
    void emptyHasZeroedCountersNoDatesAndNoFees() {
        NetPosition empty = NetPosition.empty();

        assertEquals(0L, empty.netQuantity());
        assertEquals(0L, empty.grossLong());
        assertEquals(0L, empty.grossShort());
        assertEquals(0, empty.tradeCount());
        assertNull(empty.firstTransactionDate());
        assertNull(empty.lastTransactionDate());
        assertNull(empty.lastUpdatedAt());
        assertTrue(empty.feesByCurrency().isEmpty());
    }

    @Test
    void plusAccumulatesNetAndGrossQuantitiesSeparately() {
        NetPosition position = NetPosition.empty()
                .plus(TestTransactions.transaction(500, 0, DAY_1), T1)
                .plus(TestTransactions.transaction(0, 500, DAY_1), T2);

        // The ambiguous-zero case this field exists to disambiguate: net is flat
        // but 1000 contracts moved.
        assertEquals(0L, position.netQuantity());
        assertEquals(500L, position.grossLong());
        assertEquals(500L, position.grossShort());
        assertEquals(2, position.tradeCount());
    }

    @Test
    void netQuantityIsLongMinusShort() {
        NetPosition position = NetPosition.empty()
                .plus(TestTransactions.transaction(10, 3, DAY_1), T1);

        assertEquals(7L, position.netQuantity());
    }

    @Test
    void plusTracksTheEarliestAndLatestTransactionDateRegardlessOfArrivalOrder() {
        NetPosition position = NetPosition.empty()
                .plus(TestTransactions.transaction(1, 0, DAY_2), T1)
                .plus(TestTransactions.transaction(1, 0, DAY_1), T2);

        assertEquals(DAY_1, position.firstTransactionDate());
        assertEquals(DAY_2, position.lastTransactionDate());
    }

    @Test
    void plusStampsLastUpdatedAtWithTheSuppliedObservationTime() {
        NetPosition position = NetPosition.empty()
                .plus(TestTransactions.transaction(1, 0, DAY_1), T1)
                .plus(TestTransactions.transaction(1, 0, DAY_1), T2);

        assertEquals(T2, position.lastUpdatedAt());
    }

    @Test
    void feesAccumulatePerCurrencyAndAreNeverBlended() {
        NetPosition position = NetPosition.empty()
                .plus(TestTransactions.transaction(1, 0, DAY_1,
                        new BigDecimal("-0.60"), "USD",
                        new BigDecimal("-0.30"), "USD",
                        new BigDecimal("-120.00"), "JPY"), T1);

        // Two USD fees summed; JPY kept separate. No cross-currency addition.
        assertEquals(new BigDecimal("-0.90"), position.feesByCurrency().get("USD"));
        assertEquals(new BigDecimal("-120.00"), position.feesByCurrency().get("JPY"));
        assertEquals(2, position.feesByCurrency().size());
    }

    @Test
    void feesAccumulateAcrossTransactions() {
        NetPosition position = NetPosition.empty()
                .plus(TestTransactions.transaction(1, 0, DAY_1,
                        new BigDecimal("-0.60"), "USD", BigDecimal.ZERO, "USD",
                        BigDecimal.ZERO, "JPY"), T1)
                .plus(TestTransactions.transaction(1, 0, DAY_1,
                        new BigDecimal("-0.15"), "USD", BigDecimal.ZERO, "USD",
                        BigDecimal.ZERO, "JPY"), T2);

        assertEquals(new BigDecimal("-0.75"), position.feesByCurrency().get("USD"));
        assertEquals(1, position.feesByCurrency().size());
    }

    @Test
    void zeroAmountFeesDoNotCreateMapEntries() {
        // Commission is 0.00 throughout the sample data; an all-zero JPY entry
        // would be noise in the fee KPI tile.
        NetPosition position = NetPosition.empty()
                .plus(TestTransactions.transaction(1, 0, DAY_1,
                        new BigDecimal("-0.60"), "USD", BigDecimal.ZERO, "USD",
                        BigDecimal.ZERO, "JPY"), T1);

        assertEquals(Map.of("USD", new BigDecimal("-0.60")), position.feesByCurrency());
    }

    @Test
    void blankCurrencyCodesAreSkipped() {
        NetPosition position = NetPosition.empty()
                .plus(TestTransactions.transaction(1, 0, DAY_1,
                        new BigDecimal("-0.60"), "", BigDecimal.ZERO, "USD",
                        BigDecimal.ZERO, "JPY"), T1);

        assertTrue(position.feesByCurrency().isEmpty());
    }

    @Test
    void plusDoesNotMutateTheReceiver() {
        NetPosition first = NetPosition.empty().plus(TestTransactions.transaction(5, 0, DAY_1), T1);
        NetPosition second = first.plus(TestTransactions.transaction(5, 0, DAY_1), T2);

        assertEquals(5L, first.netQuantity());
        assertEquals(10L, second.netQuantity());
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `mvn -q -pl common test -Dtest=NetPositionTest`
Expected: FAIL — compilation error, `cannot find symbol: class NetPosition`.

- [ ] **Step 4: Write the implementation**

Create `common/src/main/java/com/pfm/common/domain/NetPosition.java`:

```java
package com.pfm.common.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The running aggregate for one {@link ReportKey}.
 *
 * <p>{@code netQuantity} is the reported figure — {@code sum(quantityLong -
 * quantityShort)}. The gross figures exist because a net of zero is otherwise
 * ambiguous: no activity, or offsetting activity.
 *
 * <p>{@code lastUpdatedAt} is <em>processing</em> time, supplied by the caller,
 * not event time. A state-store rebuild re-stamps every row with the rebuild
 * time. That is correct for its only purpose — driving the UI's changed-row
 * highlight — but it is not a business timestamp; {@link #lastTransactionDate()}
 * is the event-time field.
 *
 * <p>Fees are held <em>per currency</em>. Each of the three fee fields carries
 * its own currency code, so a single blended total would risk adding USD to JPY.
 * Keying by currency removes that possibility by construction rather than
 * flagging it after the fact.
 */
public record NetPosition(
        long netQuantity,
        long grossLong,
        long grossShort,
        int tradeCount,
        LocalDate firstTransactionDate,
        LocalDate lastTransactionDate,
        Instant lastUpdatedAt,
        Map<String, BigDecimal> feesByCurrency) {

    private static final NetPosition EMPTY =
            new NetPosition(0L, 0L, 0L, 0, null, null, null, Map.of());

    public static NetPosition empty() {
        return EMPTY;
    }

    public NetPosition plus(FutureTransaction transaction, Instant observedAt) {
        Map<String, BigDecimal> fees = new LinkedHashMap<>(feesByCurrency);
        addFee(fees, transaction.exchBrokerFeeCurrency(), transaction.exchBrokerFee());
        addFee(fees, transaction.clearingFeeCurrency(), transaction.clearingFee());
        addFee(fees, transaction.commissionCurrency(), transaction.commission());

        return new NetPosition(
                netQuantity + (transaction.quantityLong() - transaction.quantityShort()),
                grossLong + transaction.quantityLong(),
                grossShort + transaction.quantityShort(),
                tradeCount + 1,
                earliest(firstTransactionDate, transaction.transactionDate()),
                latest(lastTransactionDate, transaction.transactionDate()),
                observedAt,
                Map.copyOf(fees));
    }

    private static void addFee(Map<String, BigDecimal> fees, String currency, BigDecimal amount) {
        if (currency == null || currency.isBlank() || amount == null || amount.signum() == 0) {
            return;
        }
        fees.merge(currency, amount, BigDecimal::add);
    }

    private static LocalDate earliest(LocalDate current, LocalDate candidate) {
        if (candidate == null) {
            return current;
        }
        return current == null || candidate.isBefore(current) ? candidate : current;
    }

    private static LocalDate latest(LocalDate current, LocalDate candidate) {
        if (candidate == null) {
            return current;
        }
        return current == null || candidate.isAfter(current) ? candidate : current;
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -q -pl common test -Dtest=NetPositionTest`
Expected: PASS — 10 tests.

- [ ] **Step 6: Run the whole `common` suite for regressions**

Run: `mvn -q -pl common test`
Expected: PASS — existing parser and golden-sample tests unaffected.

- [ ] **Step 7: Commit**

```bash
git add common/src/main/java/com/pfm/common/domain/NetPosition.java \
        common/src/test/java/com/pfm/common/domain/NetPositionTest.java \
        common/src/test/java/com/pfm/common/domain/TestTransactions.java
git commit -m "feat(common): add NetPosition with gross quantities and per-currency fees"
```

---

## Task 3: Point `KafkaKeyBuilder` at `ReportKey`

**Files:**
- Modify: `ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaKeyBuilder.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaKeyBuilderTest.java`

**Interfaces:**
- Consumes: `ReportKey.from(FutureTransaction)`, `ReportKey.encode()` (Task 1).
- Produces: `KafkaKeyBuilder.buildKey(FutureTransaction) -> String` — same
  signature as today, new 8-part output format.

This is the change that makes ingestion write the richer key. It is a
behavioural change to the wire format, which is why the store must be torn down
(Task 7).

- [ ] **Step 1: Update the test to expect the 8-part key**

Replace the assertion in
`ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaKeyBuilderTest.java`.
Change the method name and the expected value; leave the `FutureTransaction`
construction exactly as it is:

```java
    @Test
    void buildsKeyCarryingAllEightGroupingFields() {
        FutureTransaction transaction = new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B', 1L, 0L,
                new BigDecimal("-0.60"), "USD", 'D',
                new BigDecimal("-0.30"), "USD", 'D',
                new BigDecimal("0.00"), "JPY", 'D',
                LocalDate.of(2010, 8, 20), "001238", "0", "688032",
                new BigDecimal("9250.0000000"), "", "", 'O'
        );

        // 8 parts, not the previous 2 — the sub-fields must survive the round trip
        // so processing-service can expose them as columns.
        assertEquals("CL|4321|0002|0001|SGX|FU|NK|20100910", KafkaKeyBuilder.buildKey(transaction));
    }

    @Test
    void keyDecodesBackToTheDerivedReportColumns() {
        FutureTransaction transaction = new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B', 1L, 0L,
                new BigDecimal("-0.60"), "USD", 'D',
                new BigDecimal("-0.30"), "USD", 'D',
                new BigDecimal("0.00"), "JPY", 'D',
                LocalDate.of(2010, 8, 20), "001238", "0", "688032",
                new BigDecimal("9250.0000000"), "", "", 'O'
        );

        ReportKey decoded = ReportKey.decode(KafkaKeyBuilder.buildKey(transaction));

        assertEquals("CL432100020001", decoded.clientInformation());
        assertEquals("SGXFUNK20100910", decoded.productInformation());
    }
```

Add the import `import com.pfm.common.domain.ReportKey;` at the top.

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl ingestion-service -am test -Dtest=KafkaKeyBuilderTest`
Expected: FAIL — first test gets `CL432100020001|SGXFUNK20100910`, expected
`CL|4321|0002|0001|SGX|FU|NK|20100910`.

- [ ] **Step 3: Rewrite `KafkaKeyBuilder` to delegate**

Replace the whole body of
`ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaKeyBuilder.java`:

```java
package com.pfm.ingestion.kafka;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.ReportKey;

/**
 * Builds the Kafka message key. The format itself lives in {@link ReportKey} so
 * that this writer and processing-service's reader cannot drift apart.
 */
public final class KafkaKeyBuilder {

    private KafkaKeyBuilder() {
    }

    public static String buildKey(FutureTransaction transaction) {
        return ReportKey.from(transaction).encode();
    }
}
```

Note the `DateTimeFormatter` import is no longer needed — remove it.

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl ingestion-service -am test -Dtest=KafkaKeyBuilderTest`
Expected: PASS — 2 tests.

- [ ] **Step 5: Run the full ingestion suite**

Run: `mvn -q -pl ingestion-service -am test`
Expected: PASS. If `IngestionServiceTest` or `IngestionEndToEndTest` asserts on
key strings, update those expectations to the 8-part form — the values are the
same fields, differently delimited.

- [ ] **Step 7: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaKeyBuilder.java \
        ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaKeyBuilderTest.java
git commit -m "feat(ingestion-service): publish the 8-field ReportKey as the message key"
```

---

## Task 4: `NetPositionSerde`

**Files:**
- Create: `processing-service/src/main/java/com/pfm/processing/streams/NetPositionSerde.java`
- Test: `processing-service/src/test/java/com/pfm/processing/streams/NetPositionSerdeTest.java`

**Interfaces:**
- Consumes: `NetPosition` (Task 2).
- Produces: `NetPositionSerde.instance() -> Serde<NetPosition>`. Task 5 uses it.

Mirrors the existing `TransactionSerde` exactly — same `ObjectMapper`
configuration, so `LocalDate` and `Instant` serialize as ISO strings rather than
numeric timestamps.

- [ ] **Step 1: Write the failing test**

Create `processing-service/src/test/java/com/pfm/processing/streams/NetPositionSerdeTest.java`:

```java
package com.pfm.processing.streams;

import com.pfm.common.domain.NetPosition;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetPositionSerdeTest {

    private static final String TOPIC = "future-transactions";

    @Test
    void roundTripsEveryField() {
        NetPosition original = new NetPosition(
                -215L, 285L, 500L, 12,
                LocalDate.of(2010, 8, 19), LocalDate.of(2010, 8, 20),
                Instant.parse("2026-08-12T14:31:52Z"),
                Map.of("USD", new BigDecimal("-0.90")));

        Serde<NetPosition> serde = NetPositionSerde.instance();
        NetPosition restored = serde.deserializer()
                .deserialize(TOPIC, serde.serializer().serialize(TOPIC, original));

        assertEquals(original, restored);
    }

    @Test
    void roundTripsAnEmptyPositionWithNullDates() {
        Serde<NetPosition> serde = NetPositionSerde.instance();
        NetPosition restored = serde.deserializer()
                .deserialize(TOPIC, serde.serializer().serialize(TOPIC, NetPosition.empty()));

        assertEquals(NetPosition.empty(), restored);
    }

    @Test
    void serializesTemporalFieldsAsIsoStringsNotNumericTimestamps() {
        NetPosition position = new NetPosition(1L, 1L, 0L, 1,
                LocalDate.of(2010, 8, 19), LocalDate.of(2010, 8, 19),
                Instant.parse("2026-08-12T14:31:52Z"), Map.of());

        String json = new String(NetPositionSerde.instance().serializer().serialize(TOPIC, position));

        assertTrue(json.contains("\"2010-08-19\""), json);
        assertTrue(json.contains("2026-08-12T14:31:52Z"), json);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl processing-service -am test -Dtest=NetPositionSerdeTest`
Expected: FAIL — `cannot find symbol: class NetPositionSerde`.

- [ ] **Step 3: Write the implementation**

Create `processing-service/src/main/java/com/pfm/processing/streams/NetPositionSerde.java`:

```java
package com.pfm.processing.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pfm.common.domain.NetPosition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/** JSON serde for the aggregate value, configured identically to {@link TransactionSerde}. */
public final class NetPositionSerde {

    private NetPositionSerde() {
    }

    public static Serde<NetPosition> instance() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JsonSerializer<NetPosition> serializer = new JsonSerializer<>(objectMapper);
        JsonDeserializer<NetPosition> deserializer =
                new JsonDeserializer<>(NetPosition.class, objectMapper, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl processing-service -am test -Dtest=NetPositionSerdeTest`
Expected: PASS — 3 tests.

- [ ] **Step 5: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/streams/NetPositionSerde.java \
        processing-service/src/test/java/com/pfm/processing/streams/NetPositionSerdeTest.java
git commit -m "feat(processing-service): add NetPositionSerde for the enriched aggregate"
```

---

## Task 5: Aggregate into `NetPosition` with an injected `Clock`

**Files:**
- Modify: `processing-service/src/main/java/com/pfm/processing/streams/AggregationTopology.java`
- Test: `processing-service/src/test/java/com/pfm/processing/streams/AggregationTopologyTest.java`

**Interfaces:**
- Consumes: `NetPosition.empty()`, `NetPosition.plus(FutureTransaction, Instant)`
  (Task 2); `NetPositionSerde.instance()` (Task 4).
- Produces: `AggregationTopology.NET_QUANTITY_STORE` (unchanged constant value
  `"net-quantity-store"`) now typed `KeyValueStore<String, NetPosition>`;
  `AggregationTopology.build(StreamsBuilder, String topic, Clock clock)` — note
  the **new third parameter**. Task 6's `ReportService` reads the new store type.

**Why a `Clock` rather than a timestamp-carrying wrapper:** `Aggregator` has no
`ProcessorContext`, so the observation time must come from outside. Injecting a
`Clock` keeps the topology's generic types unchanged, needs no extra wrapper
record or serde, and makes `lastUpdatedAt` deterministic in tests via
`Clock.fixed`. Semantics are as the spec describes: processing time, re-stamped
on a store rebuild.

`DedupProcessor` is **not** modified by this task.

- [ ] **Step 1: Update the existing test's setup and assertions**

In `processing-service/src/test/java/com/pfm/processing/streams/AggregationTopologyTest.java`:

Add imports:

```java
import com.pfm.common.domain.NetPosition;
import java.time.Clock;
import java.time.ZoneOffset;
```

Add a fixed clock constant next to the existing constants:

```java
    private static final Instant NOW = Instant.parse("2026-08-12T14:31:52Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
```

Change the `build` call in `setUp()`:

```java
        AggregationTopology.build(builder, TOPIC, FIXED_CLOCK);
```

Update the two existing store assertions to read `NetPosition`:

```java
    @Test
    void aggregatesNetQuantityPerKeyAcrossMultipleRecords() {
        pipeInput(KEY, transaction(100, 30), "tx-1"); // net +70
        pipeInput(KEY, transaction(50, 80), "tx-2");   // net -30

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(40L, store.get(KEY).netQuantity());
    }

    @Test
    void duplicateTransactionIdDoesNotDoubleCountInTheAggregate() {
        FutureTransaction transaction = transaction(100, 30); // net +70

        pipeInput(KEY, transaction, "tx-1");
        pipeInput(KEY, transaction, "tx-1"); // retried/re-published duplicate

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(70L, store.get(KEY).netQuantity());
        assertEquals(1, store.get(KEY).tradeCount());
    }
```

Then add the new coverage:

```java
    @Test
    void tracksGrossQuantitiesAndTradeCountAlongsideNet() {
        pipeInput(KEY, transaction(500, 0), "tx-1");
        pipeInput(KEY, transaction(0, 500), "tx-2");

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        NetPosition position = store.get(KEY);

        assertEquals(0L, position.netQuantity());
        assertEquals(500L, position.grossLong());
        assertEquals(500L, position.grossShort());
        assertEquals(2, position.tradeCount());
    }

    @Test
    void stampsLastUpdatedAtFromTheInjectedClock() {
        pipeInput(KEY, transaction(100, 30), "tx-1");

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(NOW, store.get(KEY).lastUpdatedAt());
    }
```

Any other assertion in this file reading `store.get(...)` as a `Long` must
likewise become `.netQuantity()`. The `assertNull` import stays in use for the
"unknown key" test, whose `store.get(otherKey)` is still null.

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl processing-service -am test -Dtest=AggregationTopologyTest`
Expected: FAIL — compilation error: `build` cannot be applied to 3 arguments.

- [ ] **Step 3: Update the topology**

In `processing-service/src/main/java/com/pfm/processing/streams/AggregationTopology.java`,
add imports:

```java
import com.pfm.common.domain.NetPosition;
import java.time.Clock;
```

Replace the bean method and `build`:

```java
    @Bean
    public KStream<String, FutureTransaction> netQuantityStream(StreamsBuilder streamsBuilder,
                                                                  ProcessingProperties properties) {
        return build(streamsBuilder, properties.topic(), Clock.systemUTC());
    }

    /**
     * @param clock supplies each aggregate's {@code lastUpdatedAt}. Injected rather than
     *              read from {@code Instant.now()} so tests can pin it; {@code Aggregator}
     *              has no {@code ProcessorContext} to read stream time from.
     */
    static KStream<String, FutureTransaction> build(StreamsBuilder streamsBuilder, String topic, Clock clock) {
        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DedupProcessor.STORE_NAME), Serdes.String(), Serdes.Long()));

        KStream<String, FutureTransaction> source = streamsBuilder.stream(
                topic, Consumed.with(Serdes.String(), TransactionSerde.instance()));

        KStream<String, FutureTransaction> deduped =
                source.process(DedupProcessor::new, DedupProcessor.STORE_NAME);

        deduped.groupByKey(Grouped.with(Serdes.String(), TransactionSerde.instance()))
                .aggregate(
                        NetPosition::empty,
                        (key, transaction, position) -> position.plus(transaction, clock.instant()),
                        Materialized.<String, NetPosition, KeyValueStore<Bytes, byte[]>>as(NET_QUANTITY_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(NetPositionSerde.instance()));

        return deduped;
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q -pl processing-service -am test -Dtest=AggregationTopologyTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/streams/AggregationTopology.java \
        processing-service/src/test/java/com/pfm/processing/streams/AggregationTopologyTest.java
git commit -m "feat(processing-service): aggregate into NetPosition with an injected Clock"
```

---

## Task 6: Expand `ReportEntry` and decode the key in `ReportService`

**Files:**
- Modify: `processing-service/src/main/java/com/pfm/processing/report/ReportEntry.java`
- Modify: `processing-service/src/main/java/com/pfm/processing/report/ReportService.java`
- Test: `processing-service/src/test/java/com/pfm/processing/report/ReportServiceTest.java`
- Test: `processing-service/src/test/java/com/pfm/processing/report/ReportEntryJsonTest.java` (create)

**Interfaces:**
- Consumes: `ReportKey.decode(String)` and its accessors (Task 1);
  `NetPosition` accessors (Task 2); `AggregationTopology.NET_QUANTITY_STORE`
  typed `NetPosition` (Task 5).
- Produces: `ReportEntry.of(ReportKey, NetPosition) -> ReportEntry`, the
  18-component `ReportEntry` canonical constructor, and
  `ReportService.currentReport() -> List<ReportEntry>` (signature unchanged).
  Task 7 and the frontend consume the JSON shape asserted here.

The three original accessors keep their names — `clientInformation()`,
`productInformation()`, `netQuantity()` — because `ReportController`'s CSV
builder calls them and must not change.

- [ ] **Step 1: Write the failing JSON-shape test**

Create `processing-service/src/test/java/com/pfm/processing/report/ReportEntryJsonTest.java`:

```java
package com.pfm.processing.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pfm.common.domain.NetPosition;
import com.pfm.common.domain.ReportKey;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportEntryJsonTest {

    @Test
    void serializesWithFrozenLegacyNamesAndIsoTemporalFields() {
        ReportEntry entry = ReportEntry.of(
                new ReportKey("CL", "1234", "0003", "0001", "CME", "FU", "NK.",
                        LocalDate.of(2010, 9, 10)),
                new NetPosition(-215L, 285L, 500L, 12,
                        LocalDate.of(2010, 8, 19), LocalDate.of(2010, 8, 20),
                        Instant.parse("2026-08-12T14:31:52Z"),
                        Map.of("USD", new BigDecimal("-0.90"))));

        // Use Boot's own Jackson configuration so this test reflects what the
        // controller actually emits, not a hand-rolled ObjectMapper.
        new ApplicationContextRunner()
                .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations
                        .of(JacksonAutoConfiguration.class))
                .run(context -> {
                    String json = context.getBean(ObjectMapper.class).writeValueAsString(entry);

                    // Frozen names — any consumer of the old contract still works.
                    assertTrue(json.contains("\"Client_Information\":\"CL123400030001\""), json);
                    assertTrue(json.contains("\"Product_Information\":\"CMEFUNK.20100910\""), json);
                    assertTrue(json.contains("\"Total_Transaction_Amount\":-215"), json);
                    // Decomposed dimensions the UI filters on.
                    assertTrue(json.contains("\"clientNumber\":\"1234\""), json);
                    assertTrue(json.contains("\"accountNumber\":\"0003\""), json);
                    assertTrue(json.contains("\"symbol\":\"NK.\""), json);
                    // Dates as ISO strings, not numeric timestamps.
                    assertTrue(json.contains("\"expirationDate\":\"2010-09-10\""), json);
                    assertTrue(json.contains("\"lastTransactionDate\":\"2010-08-20\""), json);
                    // Measures.
                    assertTrue(json.contains("\"grossLong\":285"), json);
                    assertTrue(json.contains("\"grossShort\":500"), json);
                    assertTrue(json.contains("\"tradeCount\":12"), json);
                    assertTrue(json.contains("\"feesByCurrency\":{\"USD\":-0.90}"), json);
                });
    }
}
```

- [ ] **Step 2: Update `ReportServiceTest` for the new store type**

In `processing-service/src/test/java/com/pfm/processing/report/ReportServiceTest.java`,
add imports:

```java
import com.pfm.common.domain.NetPosition;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
```

Add a helper inside the class:

```java
    private static NetPosition position(long netQuantity) {
        return new NetPosition(netQuantity, Math.max(netQuantity, 0), Math.max(-netQuantity, 0), 1,
                LocalDate.of(2010, 8, 20), LocalDate.of(2010, 8, 20),
                Instant.parse("2026-08-12T14:31:52Z"), Map.of());
    }
```

Rewrite the sort test to use 8-part keys and `NetPosition` values, asserting on
accessors rather than whole-record equality (the record now has 18 components):

```java
    @Test
    @SuppressWarnings("unchecked")
    void returnsEntriesSortedByClientThenProductInformation() {
        ReadOnlyKeyValueStore<String, NetPosition> store = mock(ReadOnlyKeyValueStore.class);
        when(store.all()).thenReturn(iteratorOver(List.of(
                KeyValue.pair("CL|4321|0002|0001|SGX|FU|NK|20100910", position(46L)),
                KeyValue.pair("CL|1234|0003|0001|CME|FU|NK.|20100910", position(-215L)),
                KeyValue.pair("CL|1234|0003|0001|CME|FU|N1|20100910", position(285L)),
                KeyValue.pair("CL|1234|0002|0001|SGX|FU|NK|20100910", position(-52L)))));

        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);

        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        ReportService service = new ReportService(factoryBean);
        List<ReportEntry> report = service.currentReport();

        assertEquals(4, report.size());
        assertEquals(List.of("CL123400020001", "CL123400030001", "CL123400030001", "CL432100020001"),
                report.stream().map(ReportEntry::clientInformation).toList());
        assertEquals(List.of("SGXFUNK20100910", "CMEFUN120100910", "CMEFUNK.20100910", "SGXFUNK20100910"),
                report.stream().map(ReportEntry::productInformation).toList());
        assertEquals(List.of(-52L, 285L, -215L, 46L),
                report.stream().map(ReportEntry::netQuantity).toList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void decomposesTheKeyIntoIndividualDimensionFields() {
        ReadOnlyKeyValueStore<String, NetPosition> store = mock(ReadOnlyKeyValueStore.class);
        when(store.all()).thenReturn(iteratorOver(List.of(
                KeyValue.pair("CL|1234|0003|0001|CME|FU|NK.|20100910", position(-215L)))));

        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);
        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        ReportEntry entry = new ReportService(factoryBean).currentReport().get(0);

        assertEquals("CL", entry.clientType());
        assertEquals("1234", entry.clientNumber());
        assertEquals("0003", entry.accountNumber());
        assertEquals("0001", entry.subaccountNumber());
        assertEquals("CME", entry.exchangeCode());
        assertEquals("FU", entry.productGroupCode());
        assertEquals("NK.", entry.symbol());
        assertEquals(LocalDate.of(2010, 9, 10), entry.expirationDate());
        assertEquals(285L, entry.grossLong());
    }
```

Any remaining `ReadOnlyKeyValueStore<String, Long>` or `iteratorOver` generic in
this file becomes `NetPosition`.

- [ ] **Step 3: Run both tests to verify they fail**

Run: `mvn -q -pl processing-service -am test -Dtest='ReportEntryJsonTest+ReportServiceTest'`
Expected: FAIL — `ReportEntry.of` does not exist; `ReportEntry` has 3 components.

- [ ] **Step 4: Expand `ReportEntry`**

Replace `processing-service/src/main/java/com/pfm/processing/report/ReportEntry.java`:

```java
package com.pfm.processing.report;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.pfm.common.domain.NetPosition;
import com.pfm.common.domain.ReportKey;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

/**
 * One row of the daily summary.
 *
 * <p>The first three components carry the original {@code @JsonProperty} names and
 * are the only ones the CSV uses — they are a frozen contract. Everything after
 * them is additive: decomposed dimensions the UI filters and sorts on, and
 * measures that make a row interpretable (a net of zero is ambiguous without the
 * gross figures).
 */
public record ReportEntry(
        @JsonProperty("Client_Information") String clientInformation,
        @JsonProperty("Product_Information") String productInformation,
        @JsonProperty("Total_Transaction_Amount") long netQuantity,

        String clientType,
        String clientNumber,
        String accountNumber,
        String subaccountNumber,
        String exchangeCode,
        String productGroupCode,
        String symbol,
        LocalDate expirationDate,

        long grossLong,
        long grossShort,
        int tradeCount,
        LocalDate firstTransactionDate,
        LocalDate lastTransactionDate,
        Instant lastUpdatedAt,
        Map<String, BigDecimal> feesByCurrency) {

    public static ReportEntry of(ReportKey key, NetPosition position) {
        return new ReportEntry(
                key.clientInformation(),
                key.productInformation(),
                position.netQuantity(),
                key.clientType(),
                key.clientNumber(),
                key.accountNumber(),
                key.subaccountNumber(),
                key.exchangeCode(),
                key.productGroupCode(),
                key.symbol(),
                key.expirationDate(),
                position.grossLong(),
                position.grossShort(),
                position.tradeCount(),
                position.firstTransactionDate(),
                position.lastTransactionDate(),
                position.lastUpdatedAt(),
                position.feesByCurrency());
    }
}
```

- [ ] **Step 5: Update `ReportService`**

In `processing-service/src/main/java/com/pfm/processing/report/ReportService.java`,
add imports:

```java
import com.pfm.common.domain.NetPosition;
import com.pfm.common.domain.ReportKey;
```

Replace the store lookup, iteration and `toReportEntry`:

```java
        ReadOnlyKeyValueStore<String, NetPosition> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        AggregationTopology.NET_QUANTITY_STORE, QueryableStoreTypes.keyValueStore()));

        List<ReportEntry> entries = new ArrayList<>();
        try (KeyValueIterator<String, NetPosition> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, NetPosition> entry = iterator.next();
                entries.add(ReportEntry.of(ReportKey.decode(entry.key), entry.value));
            }
        }
        entries.sort(Comparator.comparing(ReportEntry::clientInformation)
                .thenComparing(ReportEntry::productInformation));
        return entries;
    }
}
```

Delete the private `toReportEntry` method entirely — key parsing now lives in
`ReportKey.decode`, which is the whole point of Task 1.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `mvn -q -pl processing-service -am test -Dtest='ReportEntryJsonTest+ReportServiceTest'`
Expected: PASS.

- [ ] **Step 7: Run the whole processing suite**

Run: `mvn -q -pl processing-service -am test`
Expected: PASS, except `FullPipelineGoldenTest` which Task 7 addresses. If
`ReportControllerTest` constructs `ReportEntry` directly, switch it to
`ReportEntry.of(new ReportKey(...), new NetPosition(...))` so it does not have to
spell 18 components.

- [ ] **Step 8: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/report/ReportEntry.java \
        processing-service/src/main/java/com/pfm/processing/report/ReportService.java \
        processing-service/src/test/java/com/pfm/processing/report/
git commit -m "feat(processing-service): expose decomposed dimensions and measures on ReportEntry"
```

---

## Task 7: Lock the CSV against the committed fixture, document teardown

**Files:**
- Create: `processing-service/src/test/resources/Output.csv` (copy of `sample-output/Output.csv`)
- Modify: `processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java`
- Modify: `processing-service/README.md`

**Interfaces:**
- Consumes: the running end-to-end pipeline from Tasks 3–6.
- Produces: nothing consumed by later tasks. This is the contract lock.

`FullPipelineGoldenTest` already asserts the exact CSV, but as a hardcoded
string literal — so the code and the committed `sample-output/Output.csv` could
drift apart without any test noticing. Reading the fixture instead makes the
committed file the single source of truth. This follows the existing convention:
`Input.txt` is already copied into each module's `src/test/resources` and loaded
from the classpath.

- [ ] **Step 1: Copy the fixture into test resources**

```bash
cp sample-output/Output.csv processing-service/src/test/resources/Output.csv
```

- [ ] **Step 2: Replace the hardcoded literal with a fixture read**

In `processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java`,
delete the `EXPECTED_CSV` string-concatenation constant and add:

```java
    /**
     * Read from the committed fixture rather than inlined, so this test fails if the
     * pipeline's output and sample-output/Output.csv ever disagree. The copy in
     * src/test/resources must stay in sync with sample-output/Output.csv — the same
     * convention Input.txt already follows.
     */
    private static String expectedCsv() throws IOException {
        try (InputStream in = FullPipelineGoldenTest.class.getResourceAsStream("/Output.csv")) {
            if (in == null) {
                throw new IllegalStateException("Missing test fixture /Output.csv on the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
```

Add imports:

```java
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
```

Change the test signature to `throws URISyntaxException, IOException` and the
assertion at line ~90:

```java
        String csv = awaitFullReportCsv(rest);
        assertEquals(expectedCsv(), csv,
                "CSV output must stay byte-identical to sample-output/Output.csv");
```

- [ ] **Step 3: Make the golden test hermetic (do this BEFORE trusting it)**

`FullPipelineGoldenTest` never sets `state.dir`, so Kafka Streams reuses its
RocksDB directory across runs. This was observed producing an 8-row report — 3
rows in the current key format plus 5 left over from a run two days earlier. The
dangerous direction is the opposite one: if stale state happens to match the
fixture, **the test passes without exercising the pipeline at all**. Since this
test is the CSV contract lock, that must be impossible.

Give the test its own throwaway state directory. Add a `@TempDir` field and pass
it as a Streams property alongside the existing ones:

```java
    @TempDir
    static Path streamsStateDir;
```

and in the `processingContext` builder's `.properties(...)` list, add:

```java
                        "spring.kafka.streams.state-dir=" + streamsStateDir.toAbsolutePath(),
```

Import `org.junit.jupiter.api.io.TempDir` and `java.nio.file.Path`. JUnit creates
a fresh directory per run and deletes it afterwards, so the test can no longer
inherit or leak state.

Verify the fix actually bites: run the test twice in a row and confirm both runs
report the same 5 rows. Before this change, a second run could differ from the
first.

- [ ] **Step 4: Run the golden test**

Run: `mvn -q -pl processing-service -am test -Dtest=FullPipelineGoldenTest`
Expected: PASS. This is the single most important verification in the plan — it
proves the key and aggregate rewrites left the reported numbers and row order
untouched. Requires Docker (Testcontainers).

If it fails on row *order*, check that `ReportService` still sorts by
`clientInformation` then `productInformation`. If it fails on *values*, the
`plus` accumulator's `netQuantity` arithmetic is wrong.

- [ ] **Step 5: Document the teardown in the processing-service README**

Add this section to `processing-service/README.md`:

```markdown
## Upgrading past the ReportKey change

The Kafka message key changed from two concatenated fields to all eight
(`ReportKey`), and the `net-quantity-store` value changed from a `Long` to a
`NetPosition`. Neither the store name nor the `application-id` was renamed —
a version baked into an identifier outlives the migration that caused it — so
existing state must be discarded rather than migrated.

No deployment path in this repo persists state, so in practice there is nothing
to clean up: `docker-compose.yml` declares no volumes at all, and
`k8s/kafka.yaml` uses `emptyDir: {}` with no `volumeMounts` on
processing-service. Both are pod/container-lifetime only.

The one exception is the broker-only development loop (`docker compose up -d
kafka` with the services run via Maven), where Kafka Streams keeps RocksDB state
on the **host**:

```bash
docker compose down -v                                    # containerised paths
rm -rf "${TMPDIR:-/tmp}/kafka-streams/processing-service"  # broker-only loop, host-side state
```

`${TMPDIR:-/tmp}` matters: Kafka Streams derives `state.dir` from `java.io.tmpdir`,
which is `/tmp` on Linux but a per-user `/var/folders/.../T/` on macOS. A
hardcoded `/tmp/kafka-streams` silently no-ops on macOS — the exact machine where
the broker-only loop runs.

Kafka Streams defaults `auto.offset.reset` to `earliest` (unlike a plain
consumer, which defaults to `latest`), so after teardown the topic replays from
the beginning and the store rebuilds with the new key format automatically.
```

- [ ] **Step 6: Run the full backend suite**

Run: `mvn -q test`
Expected: PASS across `common`, `ingestion-service`, `processing-service`.

- [ ] **Step 7: Commit**

```bash
git add processing-service/src/test/resources/Output.csv \
        processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java \
        processing-service/README.md
git commit -m "test(processing-service): lock CSV output to the committed fixture; document teardown"
```

---

## Task 8: Record when an ingestion actually happened

**Files:**
- Modify: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionRegistry.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionRegistryTest.java`

**Interfaces:**
- Produces: `IngestionRegistry.LastIngest(Instant at, IngestionResult result)` record,
  `IngestionRegistry.lastIngest() -> Optional<LastIngest>`, and a second
  constructor `IngestionRegistry(Clock)`. Task 9 consumes `lastIngest()`.

Nothing currently records *when* an ingestion ran. The critical behaviour: a
**cache hit publishes nothing to Kafka, so it must not advance the timestamp** —
otherwise the UI's file panel would claim fresh activity where none occurred.

The existing no-arg constructor is retained (delegating to `Clock.systemUTC()`)
so Spring keeps using it and the six existing `new IngestionRegistry()` call
sites are untouched. The `Clock` constructor exists for deterministic tests.

- [ ] **Step 1: Write the failing tests**

Append to `ingestion-service/src/test/java/com/pfm/ingestion/IngestionRegistryTest.java`:

```java
    @Test
    void lastIngestIsEmptyBeforeAnythingHasBeenComputed() {
        IngestionRegistry registry = new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC));

        assertTrue(registry.lastIngest().isEmpty());
    }

    @Test
    void computingAnIngestionRecordsItsTimestampAndResult() {
        IngestionRegistry registry = new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC));
        IngestionResult result = new IngestionResult("fp", 717, 717, 0, List.of(), false);

        registry.getOrCompute("fp", () -> result);

        IngestionRegistry.LastIngest last = registry.lastIngest().orElseThrow();
        assertEquals(T1, last.at());
        assertSame(result, last.result());
    }

    @Test
    void aCacheHitDoesNotAdvanceTheLastIngestTimestamp() {
        // A cache hit republishes nothing to Kafka, so reporting it as a fresh
        // ingestion would tell the UI activity happened when none did.
        MutableClock clock = new MutableClock(T1);
        IngestionRegistry registry = new IngestionRegistry(clock);
        IngestionResult result = new IngestionResult("fp", 717, 717, 0, List.of(), false);

        registry.getOrCompute("fp", () -> result);
        clock.set(T2);
        IngestionRegistry.CacheOutcome second = registry.getOrCompute("fp", () -> result);

        assertTrue(second.cached());
        assertEquals(T1, registry.lastIngest().orElseThrow().at());
    }

    @Test
    void forceComputeAdvancesTheLastIngestTimestamp() {
        MutableClock clock = new MutableClock(T1);
        IngestionRegistry registry = new IngestionRegistry(clock);

        registry.getOrCompute("fp", () -> new IngestionResult("fp", 1, 1, 0, List.of(), false));
        clock.set(T2);
        registry.forceCompute("fp", () -> new IngestionResult("fp", 2, 2, 0, List.of(), false));

        assertEquals(T2, registry.lastIngest().orElseThrow().at());
        assertEquals(2, registry.lastIngest().orElseThrow().result().totalLines());
    }

    @Test
    void anUncachedResultStillCountsAsAnIngestionThatHappened() {
        // A batch with Kafka send failures is deliberately not cached, but records
        // WERE published, so it must still be reported as the last ingestion.
        MutableClock clock = new MutableClock(T1);
        IngestionRegistry registry = new IngestionRegistry(clock);
        IngestionResult withSendFailure = new IngestionResult("fp", 10, 4, 6,
                List.of(new ParseError(-1, "key", "Kafka send failed")), false);

        registry.getOrCompute("fp", () -> withSendFailure, result -> false);

        assertEquals(T1, registry.lastIngest().orElseThrow().at());
        assertEquals(4, registry.lastIngest().orElseThrow().result().published());
    }
```

Add these constants and helper to the class, plus imports
`java.time.Clock`, `java.time.Instant`, `java.time.ZoneId`, `java.time.ZoneOffset`:

```java
    private static final Instant T1 = Instant.parse("2026-08-12T14:31:52Z");
    private static final Instant T2 = Instant.parse("2026-08-12T15:00:00Z");

    /** A Clock whose instant can be moved, for asserting what does and does not re-stamp. */
    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void set(Instant now) {
            this.now = now;
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionRegistryTest`
Expected: FAIL — no `IngestionRegistry(Clock)` constructor, no `lastIngest()`.

- [ ] **Step 3: Implement in `IngestionRegistry`**

Add imports to `ingestion-service/src/main/java/com/pfm/ingestion/IngestionRegistry.java`:

```java
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
```

Add the record, field, constructors and accessor:

```java
    /** The most recent ingestion that actually ran, as opposed to one served from cache. */
    public record LastIngest(Instant at, IngestionResult result) {
    }

    private final AtomicReference<LastIngest> lastIngest = new AtomicReference<>();
    private final Clock clock;

    public IngestionRegistry() {
        this(Clock.systemUTC());
    }

    /** Test seam: lets a test pin or advance the timestamp deterministically. */
    IngestionRegistry(Clock clock) {
        this.clock = clock;
    }

    public Optional<LastIngest> lastIngest() {
        return Optional.ofNullable(lastIngest.get());
    }
```

In `getOrCompute(String, Supplier, Predicate)`, record the ingestion inside the
mapping function — where a compute genuinely happened — right after
`freshResult.set(result)`:

```java
        IngestionResult cached = cache.computeIfAbsent(fingerprint, fp -> {
            computed.set(true);
            IngestionResult result = computation.get();
            freshResult.set(result);
            // Records WERE published here even if the result is judged uncacheable,
            // so this counts as an ingestion that happened.
            lastIngest.set(new LastIngest(clock.instant(), result));
            // Returning null from a computeIfAbsent mapping function stores nothing,
            // leaving the key absent so the next call recomputes.
            return shouldCache.test(result) ? result : null;
        });
```

In `forceCompute`, record it too:

```java
    public IngestionResult forceCompute(String fingerprint, Supplier<IngestionResult> computation) {
        IngestionResult result = computation.get();
        lastIngest.set(new LastIngest(clock.instant(), result));
        cache.put(fingerprint, result);
        return result;
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionRegistryTest`
Expected: PASS — the 5 new tests plus all pre-existing ones, unchanged.

- [ ] **Step 5: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/IngestionRegistry.java \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionRegistryTest.java
git commit -m "feat(ingestion-service): track the last ingestion that actually ran"
```

---

## Task 9: `GET /api/v1/ingest/status`

**Files:**
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionStatus.java`
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionStatusService.java`
- Modify: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionStatusServiceTest.java`

**Interfaces:**
- Consumes: `IngestionRegistry.lastIngest()` (Task 8), `IngestionProperties.filePath()`.
- Produces: `IngestionStatus` record and
  `IngestionStatusService.currentStatus() -> IngestionStatus`; HTTP
  `GET /api/v1/ingest/status`. The frontend's `IngestionStatus` TS interface
  (Task 15) mirrors this shape field-for-field.

**Always returns 200.** "Never ingested" is a normal state expressed as null
run-fields, not an error. **`configuredPath` is the raw config value**, never
`path.toAbsolutePath()` — `IngestionController` deliberately strips absolute
paths from responses and this must not regress that.

- [ ] **Step 1: Write the failing test**

Create `ingestion-service/src/test/java/com/pfm/ingestion/IngestionStatusServiceTest.java`:

```java
package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngestionStatusServiceTest {

    private static final Instant T1 = Instant.parse("2026-08-12T14:31:52Z");

    @Test
    void reportsFileMetadataAndNullRunFieldsBeforeAnyIngestion(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("Input.txt");
        Files.writeString(file, "some content", StandardCharsets.UTF_8);
        IngestionStatusService service = new IngestionStatusService(
                new IngestionProperties(file.toString(), "future-transactions"),
                new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC)));

        IngestionStatus status = service.currentStatus();

        assertTrue(status.fileExists());
        assertEquals(12L, status.fileSizeBytes());
        assertEquals(Files.getLastModifiedTime(file).toInstant(), status.fileLastModified());
        // Never ingested is a normal state, not an error.
        assertNull(status.lastIngestAt());
        assertNull(status.fingerprint());
        assertNull(status.totalLines());
        assertNull(status.published());
        assertNull(status.skipped());
        assertNull(status.errorCount());
    }

    @Test
    void reportsTheConfiguredPathVerbatimAndNotAnAbsolutePath() {
        // The controller strips absolute paths from error responses to avoid
        // advertising container filesystem layout; this endpoint honours that.
        IngestionStatusService service = new IngestionStatusService(
                new IngestionProperties("sample-data/Input.txt", "future-transactions"),
                new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC)));

        assertEquals("sample-data/Input.txt", service.currentStatus().configuredPath());
    }

    @Test
    void reportsFileAbsentWithoutFailingWhenThePathDoesNotExist(@TempDir Path dir) {
        IngestionStatusService service = new IngestionStatusService(
                new IngestionProperties(dir.resolve("nope.txt").toString(), "future-transactions"),
                new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC)));

        IngestionStatus status = service.currentStatus();

        assertFalse(status.fileExists());
        assertNull(status.fileSizeBytes());
        assertNull(status.fileLastModified());
    }

    @Test
    void reportsCountsAndTimestampAfterAnIngestion(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("Input.txt");
        Files.writeString(file, "x", StandardCharsets.UTF_8);
        IngestionRegistry registry = new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC));
        registry.getOrCompute("fp-1", () -> new IngestionResult("fp-1", 717, 715, 2,
                List.of(new ParseError(3, "bad line", "not numeric")), false));

        IngestionStatus status = new IngestionStatusService(
                new IngestionProperties(file.toString(), "future-transactions"), registry).currentStatus();

        assertEquals(T1, status.lastIngestAt());
        assertEquals("fp-1", status.fingerprint());
        assertEquals(717, status.totalLines());
        assertEquals(715, status.published());
        assertEquals(2, status.skipped());
        // The count only — raw lines contain client data and are never exposed.
        assertEquals(1, status.errorCount());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionStatusServiceTest`
Expected: FAIL — `cannot find symbol: class IngestionStatusService`.

- [ ] **Step 3: Create the response record**

Create `ingestion-service/src/main/java/com/pfm/ingestion/IngestionStatus.java`:

```java
package com.pfm.ingestion;

import java.time.Instant;

/**
 * Provenance for the file the report is built from.
 *
 * <p>{@code configuredPath} is the {@code ingestion.file-path} config value as
 * written, deliberately not the resolved absolute path — it is what an operator
 * wants to verify and it discloses nothing about the container's filesystem.
 *
 * <p>All run fields are null until an ingestion has actually happened; that is a
 * normal state, not an error, and the endpoint still returns 200.
 */
public record IngestionStatus(
        String configuredPath,
        boolean fileExists,
        Long fileSizeBytes,
        Instant fileLastModified,
        Instant lastIngestAt,
        String fingerprint,
        Integer totalLines,
        Integer published,
        Integer skipped,
        Integer errorCount) {
}
```

- [ ] **Step 4: Create the service**

Create `ingestion-service/src/main/java/com/pfm/ingestion/IngestionStatusService.java`:

```java
package com.pfm.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

@Service
public class IngestionStatusService {

    private static final Logger log = LoggerFactory.getLogger(IngestionStatusService.class);

    private final IngestionProperties properties;
    private final IngestionRegistry registry;

    public IngestionStatusService(IngestionProperties properties, IngestionRegistry registry) {
        this.properties = properties;
        this.registry = registry;
    }

    public IngestionStatus currentStatus() {
        Path path = Path.of(properties.filePath());
        boolean exists = Files.exists(path);

        Long sizeBytes = null;
        Instant lastModified = null;
        if (exists) {
            try {
                sizeBytes = Files.size(path);
                lastModified = Files.getLastModifiedTime(path).toInstant();
            } catch (IOException e) {
                // Readable-then-unreadable is a race, not a client error: report the
                // file as present with unknown metadata rather than failing the call.
                log.warn("Could not read metadata for ingestion file: {}", e.getMessage());
            }
        }

        Optional<IngestionRegistry.LastIngest> last = registry.lastIngest();
        return new IngestionStatus(
                properties.filePath(),
                exists,
                sizeBytes,
                lastModified,
                last.map(IngestionRegistry.LastIngest::at).orElse(null),
                last.map(l -> l.result().fingerprint()).orElse(null),
                last.map(l -> l.result().totalLines()).orElse(null),
                last.map(l -> l.result().published()).orElse(null),
                last.map(l -> l.result().skipped()).orElse(null),
                last.map(l -> l.result().errors().size()).orElse(null));
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionStatusServiceTest`
Expected: PASS — 4 tests.

- [ ] **Step 6: Wire up the endpoint**

In `ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java`,
add the status service to the constructor and add the mapping:

```java
    private final IngestionService ingestionService;
    private final IngestionStatusService statusService;

    public IngestionController(IngestionService ingestionService, IngestionStatusService statusService) {
        this.ingestionService = ingestionService;
        this.statusService = statusService;
    }

    @GetMapping("/ingest/status")
    public IngestionStatus status() {
        return statusService.currentStatus();
    }
```

The wildcard import `org.springframework.web.bind.annotation.*` already covers
`@GetMapping`.

- [ ] **Step 7: Add a controller test**

Append to `ingestion-service/src/test/java/com/pfm/ingestion/IngestionControllerTest.java`
(match the file's existing MockMvc/mocking style — if it uses
`@WebMvcTest`, add `IngestionStatusService` as a `@MockitoBean`):

```java
    @Test
    void statusEndpointReturns200WithTheConfiguredPath() throws Exception {
        when(statusService.currentStatus()).thenReturn(new IngestionStatus(
                "sample-data/Input.txt", true, 127624L,
                Instant.parse("2026-08-12T09:14:00Z"),
                Instant.parse("2026-08-12T14:31:52Z"), "fp-1", 717, 717, 0, 0));

        mockMvc.perform(get("/api/v1/ingest/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.configuredPath").value("sample-data/Input.txt"))
                .andExpect(jsonPath("$.published").value(717));
    }
```

- [ ] **Step 8: Run the full ingestion suite**

Run: `mvn -q -pl ingestion-service -am test`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/IngestionStatus.java \
        ingestion-service/src/main/java/com/pfm/ingestion/IngestionStatusService.java \
        ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java \
        ingestion-service/src/test/java/com/pfm/ingestion/
git commit -m "feat(ingestion-service): expose read-only file provenance at GET /ingest/status"
```

---

## Task 10: Route only the status endpoint to ingestion-service

**Files:**
- Modify: `frontend/nginx.conf.template`
- Modify: `frontend/proxy.conf.json`
- Modify: `docker-compose.yml`
- Modify: `k8s/frontend.yaml`

**No `Dockerfile` change is needed.** `frontend/Dockerfile` copies the template to
`/etc/nginx/templates/default.conf.template` and relies on the nginx image's own
`20-envsubst-on-templates.sh` entrypoint, which substitutes **every** environment
variable present — there is no explicit variable list to extend. Setting
`INGESTION_SERVICE_UPSTREAM` in the environment is sufficient.

**Interfaces:**
- Consumes: `GET /api/v1/ingest/status` (Task 9).
- Produces: browser-reachable `/api/v1/ingest/status`. Task 15's
  `IngestionStatusService` (Angular) calls this path.

**Read-only is enforced by the route shape, not by a rule.** `location =` is an
*exact* match, so `POST /api/v1/ingest` does not match it, falls through to the
`/api/` prefix location → processing-service, and 404s. Nobody can forget to
enforce this later.

- [ ] **Step 1: Add the exact-match location to nginx**

Edit `frontend/nginx.conf.template` — the new block goes **before** `location /api/`:

```nginx
server {
    listen 80;
    root /usr/share/nginx/html;
    index index.html;
    resolver ${NGINX_LOCAL_RESOLVERS};

    # Exact match, deliberately: POST /api/v1/ingest does NOT match this location,
    # so it falls through to /api/ -> processing-service and 404s. The UI is a
    # viewer and cannot trigger ingestion, enforced by routing rather than policy.
    location = /api/v1/ingest/status {
        set $ingestion ${INGESTION_SERVICE_UPSTREAM};
        proxy_pass $ingestion;
        proxy_set_header Host $host;
    }

    location /api/ {
        set $upstream ${PROCESSING_SERVICE_UPSTREAM};
        proxy_pass $upstream;
        proxy_set_header Host $host;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
```

- [ ] **Step 2: Set the variable in docker-compose**

In `docker-compose.yml`, under the `frontend` service's `environment:` block, add:

```yaml
      INGESTION_SERVICE_UPSTREAM: http://ingestion-service:8081
```

- [ ] **Step 3: Set the variable in the k8s deployment**

In `k8s/frontend.yaml`, alongside the existing `PROCESSING_SERVICE_UPSTREAM` env
entry on the container:

```yaml
            - name: INGESTION_SERVICE_UPSTREAM
              value: "http://ingestion-service.pfm.svc.cluster.local:8081"
```

**The FQDN is mandatory, not stylistic.** `proxy_pass $variable` makes nginx
resolve the host at request time through its own `resolver`, and nginx's resolver
does **not** apply the `search` suffixes from `/etc/resolv.conf` — only
glibc-based resolvers do. A bare `ingestion-service` is a single-label query that
falls outside CoreDNS's `cluster.local` zone, returns NXDOMAIN, and yields 502 on
every status request in-cluster. Docker Compose masks this completely: its
embedded DNS resolves bare service names directly, so a Compose-only check passes
while k8s is broken.

This repo has already been bitten twice — `01d1633` ("use FQDN for
processing-service upstream") and `736b6f4`, whose message states it outright:
"The short name that Docker resolves does not work in Kubernetes either, because
nginx's resolver skips search-domain expansion." Match the FQDN form the
`PROCESSING_SERVICE_UPSTREAM` entry beside it already uses.

- [ ] **Step 4: Add the dev-server proxy entry**

Replace `frontend/proxy.conf.json`. The more specific key must come first:

```json
{
  "/api/v1/ingest": {
    "target": "http://localhost:8081",
    "secure": false
  },
  "/api": {
    "target": "http://localhost:8082",
    "secure": false
  }
}
```

- [ ] **Step 5: Verify routing end to end**

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
curl -X POST http://localhost:8081/api/v1/ingest
```

Then check all three behaviours through the frontend origin:

```bash
# Reaches ingestion-service: expect 200 and a configuredPath field
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/api/v1/ingest/status
curl -s http://localhost:8080/api/v1/ingest/status

# Still reaches processing-service
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/api/v1/report

# Must be 404 -- exact-match means this never reaches ingestion-service
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://localhost:8080/api/v1/ingest
```

Expected: `200`, a JSON body containing `"configuredPath":"sample-data/Input.txt"`,
`200`, then `404`. If nginx failed to start, `docker compose logs frontend` will
show an unresolved `${INGESTION_SERVICE_UPSTREAM}`, meaning the environment
variable never reached the container — revisit Step 2.

- [ ] **Step 6: Commit**

```bash
git add frontend/nginx.conf.template frontend/proxy.conf.json \
        docker-compose.yml k8s/frontend.yaml
git commit -m "feat(frontend): route GET /api/v1/ingest/status to ingestion-service only"
```

---

## Task 11: Tailwind v4 with the validated palette as tokens

**Files:**
- Modify: `frontend/package.json` (via `npm install`)
- Create: `frontend/.postcssrc.json`
- Modify: `frontend/src/styles.css`
- Create: `frontend/src/app/shared/local-preference.ts`
- Create: `frontend/src/app/shared/theme.ts`
- Create: `frontend/src/app/shared/theme-toggle.ts`
- Test: `frontend/src/app/shared/theme.spec.ts`
- Test: `frontend/src/app/shared/local-preference.spec.ts`

**Interfaces:**
- Produces:
  - `readPreference<T>(key: string, fallback: T): T` and
    `writePreference<T>(key: string, value: T): void` — used by Tasks 13, 16.
  - `ThemeStore` (injectable, `providedIn: 'root'`) with
    `theme: Signal<'light' | 'dark' | 'auto'>` and `cycle(): void`.
  - `ThemeToggle` standalone component, selector `app-theme-toggle`.
  - CSS custom properties consumed by every later frontend task:
    `--surface-1`, `--surface-page`, `--ink-primary`, `--ink-secondary`,
    `--ink-muted`, `--rule`, `--net-long`, `--net-short`, `--net-flat`,
    `--status-good`, `--status-warning`, `--status-critical`.

**Colour provenance:** every value below comes from the `dataviz` skill's
reference palette and was verified with `scripts/validate_palette.js`. The
diverging pair passes all six checks in both modes — worst-pair CVD ΔE 21.6
(protan) light / 19.2 dark against a ≥8 target, normal-vision 32.3 / 29.0 against
a ≥15 floor. **Do not substitute other hexes without re-running that validator.**

Dark values are the palette's own dark steps, not an inversion. The scoping
pattern below is deliberate: the `:where(:not([data-theme="light"]))` guard lets
an explicit light choice beat OS-dark, and the `[data-theme="dark"]` block lets an
explicit dark choice beat OS-light — the toggle must win in both directions.

- [ ] **Step 1: Install Tailwind v4**

```bash
cd frontend && npm install -D tailwindcss @tailwindcss/postcss postcss
```

- [ ] **Step 2: Add the PostCSS config**

Create `frontend/.postcssrc.json`:

```json
{
  "plugins": {
    "@tailwindcss/postcss": {}
  }
}
```

Angular 21's `@angular/build:application` builder discovers `.postcssrc.json`
automatically — `angular.json` needs no change.

- [ ] **Step 3: Write the token layer**

Replace `frontend/src/styles.css` entirely:

```css
@import "tailwindcss";

/*
 * Colour tokens from the dataviz reference palette, validated with
 * scripts/validate_palette.js: the diverging pair clears every check in both
 * modes (worst-pair CVD ΔE 21.6 protan light / 19.2 dark against a >=8 target).
 * Do not swap these hexes without re-running that validator.
 *
 * Net quantity is polarity data -- net long vs net short around a zero baseline --
 * so its colour job is diverging (blue <-> red, neutral gray midpoint), not
 * categorical and not sequential.
 */
:root {
  color-scheme: light;
  --surface-1: #fcfcfb;
  --surface-page: #f9f9f7;
  --ink-primary: #0b0b0b;
  --ink-secondary: #52514e;
  --ink-muted: #898781;
  --rule: #e1e0d9;
  --net-long: #2a78d6;
  --net-short: #e34948;
  --net-flat: #f0efec;
  --status-good: #0ca30c;
  --status-warning: #fab219;
  --status-critical: #d03b3b;
}

/* OS preference, but an explicit light choice must still win. */
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) {
    color-scheme: dark;
    --surface-1: #1a1a19;
    --surface-page: #0d0d0d;
    --ink-primary: #ffffff;
    --ink-secondary: #c3c2b7;
    --ink-muted: #898781;
    --rule: #2c2c2a;
    --net-long: #3987e5;
    --net-short: #e66767;
    --net-flat: #383835;
  }
}

/* Explicit dark choice must beat OS-light. */
:root[data-theme="dark"] {
  color-scheme: dark;
  --surface-1: #1a1a19;
  --surface-page: #0d0d0d;
  --ink-primary: #ffffff;
  --ink-secondary: #c3c2b7;
  --ink-muted: #898781;
  --rule: #2c2c2a;
  --net-long: #3987e5;
  --net-short: #e66767;
  --net-flat: #383835;
}

/*
 * @theme inline (not plain @theme) so the generated utilities REFERENCE the
 * custom properties rather than inlining their values -- required for the
 * light/dark swap above to take effect on utility classes.
 */
@theme inline {
  --color-surface-1: var(--surface-1);
  --color-surface-page: var(--surface-page);
  --color-ink-primary: var(--ink-primary);
  --color-ink-secondary: var(--ink-secondary);
  --color-ink-muted: var(--ink-muted);
  --color-rule: var(--rule);
  --color-net-long: var(--net-long);
  --color-net-short: var(--net-short);
  --color-net-flat: var(--net-flat);
  --color-status-good: var(--status-good);
  --color-status-warning: var(--status-warning);
  --color-status-critical: var(--status-critical);
  --font-sans: system-ui, -apple-system, "Segoe UI", sans-serif;
}

body {
  background: var(--surface-page);
  color: var(--ink-primary);
  font-family: var(--font-sans);
}

/* Columns of figures must align vertically; standalone display numbers need not. */
.tabular {
  font-variant-numeric: tabular-nums;
}

@media (prefers-reduced-motion: reduce) {
  *,
  *::before,
  *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

- [ ] **Step 4: Write the failing preference-helper test**

Create `frontend/src/app/shared/local-preference.spec.ts`:

```ts
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { readPreference, writePreference } from './local-preference';

describe('local-preference', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('returns the fallback when nothing is stored', () => {
    expect(readPreference('pfm.missing', 'default')).toBe('default');
  });

  it('round-trips a value', () => {
    writePreference('pfm.key', { a: 1 });
    expect(readPreference('pfm.key', null)).toEqual({ a: 1 });
  });

  it('returns the fallback when the stored value is not valid JSON', () => {
    localStorage.setItem('pfm.broken', '{not json');
    expect(readPreference('pfm.broken', 'fallback')).toBe('fallback');
  });

  it('does not throw when storage is unavailable', () => {
    // Private browsing can make setItem throw; a preference is never worth a crash.
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('QuotaExceededError');
    });
    expect(() => writePreference('pfm.key', 'v')).not.toThrow();
    vi.restoreAllMocks();
  });
});
```

- [ ] **Step 5: Implement the preference helpers**

Create `frontend/src/app/shared/local-preference.ts`:

```ts
/**
 * localStorage access that never throws. A stored preference is a convenience;
 * private-browsing quota errors or hand-corrupted values must degrade to the
 * caller's fallback rather than break the page.
 */
export function readPreference<T>(key: string, fallback: T): T {
  try {
    const raw = localStorage.getItem(key);
    return raw === null ? fallback : (JSON.parse(raw) as T);
  } catch {
    return fallback;
  }
}

export function writePreference<T>(key: string, value: T): void {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Preferences are best-effort.
  }
}
```

- [ ] **Step 6: Write the failing theme test**

Create `frontend/src/app/shared/theme.spec.ts`:

```ts
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { ThemeStore } from './theme';

describe('ThemeStore', () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.removeAttribute('data-theme');
    TestBed.resetTestingModule();
  });

  it('defaults to auto and stamps no attribute', () => {
    const store = TestBed.inject(ThemeStore);

    expect(store.theme()).toBe('auto');
    expect(document.documentElement.hasAttribute('data-theme')).toBe(false);
  });

  it('cycles light -> dark -> auto', () => {
    const store = TestBed.inject(ThemeStore);

    store.cycle();
    expect(store.theme()).toBe('light');
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');

    store.cycle();
    expect(store.theme()).toBe('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

    store.cycle();
    expect(store.theme()).toBe('auto');
    // auto must REMOVE the attribute so the media query governs again.
    expect(document.documentElement.hasAttribute('data-theme')).toBe(false);
  });

  it('restores a persisted choice on construction', () => {
    localStorage.setItem('pfm.theme', '"dark"');

    const store = TestBed.inject(ThemeStore);

    expect(store.theme()).toBe('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });
});
```

- [ ] **Step 7: Run both tests to verify they fail**

Run: `cd frontend && npx vitest run src/app/shared`
Expected: FAIL — cannot resolve `./local-preference` and `./theme`.

- [ ] **Step 8: Implement the theme store and toggle**

Create `frontend/src/app/shared/theme.ts`:

```ts
import { Injectable, signal } from '@angular/core';
import { readPreference, writePreference } from './local-preference';

export type Theme = 'light' | 'dark' | 'auto';

const STORAGE_KEY = 'pfm.theme';
const ORDER: readonly Theme[] = ['auto', 'light', 'dark'];

@Injectable({ providedIn: 'root' })
export class ThemeStore {
  private readonly _theme = signal<Theme>(readPreference<Theme>(STORAGE_KEY, 'auto'));

  readonly theme = this._theme.asReadonly();

  constructor() {
    this.apply(this._theme());
  }

  cycle(): void {
    const next = ORDER[(ORDER.indexOf(this._theme()) + 1) % ORDER.length];
    this._theme.set(next);
    writePreference(STORAGE_KEY, next);
    this.apply(next);
  }

  /**
   * 'auto' removes the attribute rather than setting a value, so the
   * prefers-color-scheme media query governs again. Setting data-theme="auto"
   * would match neither CSS scope and strand the page in light mode.
   */
  private apply(theme: Theme): void {
    if (theme === 'auto') {
      document.documentElement.removeAttribute('data-theme');
      return;
    }
    document.documentElement.setAttribute('data-theme', theme);
  }
}
```

Create `frontend/src/app/shared/theme-toggle.ts`:

```ts
import { Component, inject } from '@angular/core';
import { ThemeStore } from './theme';

@Component({
  selector: 'app-theme-toggle',
  template: `
    <button
      type="button"
      data-testid="theme-toggle"
      class="rounded border border-rule px-2 py-1 text-sm text-ink-secondary hover:text-ink-primary"
      [attr.aria-label]="'Theme: ' + themeStore.theme() + '. Click to change.'"
      (click)="themeStore.cycle()"
    >
      {{ label() }}
    </button>
  `,
})
export class ThemeToggle {
  protected readonly themeStore = inject(ThemeStore);

  protected label(): string {
    const theme = this.themeStore.theme();
    if (theme === 'light') return 'Light';
    if (theme === 'dark') return 'Dark';
    return 'Auto';
  }
}
```

- [ ] **Step 9: Run the tests to verify they pass**

Run: `cd frontend && npx vitest run src/app/shared`
Expected: PASS — 7 tests.

- [ ] **Step 10: Verify Tailwind actually compiles**

Run: `cd frontend && npm run build`
Expected: succeeds. Then confirm the utilities were generated rather than silently
skipped:

```bash
grep -o 'net-long' frontend/dist/frontend/browser/*.css | head -1
```

Expected: a match. No match means PostCSS did not pick up `.postcssrc.json` — the
build would still succeed with unstyled output, so this check is the one that
catches it.

- [ ] **Step 11: Commit**

```bash
git add frontend/package.json frontend/package-lock.json frontend/.postcssrc.json \
        frontend/src/styles.css frontend/src/app/shared/
git commit -m "feat(frontend): add Tailwind v4 with the validated palette as theme tokens"
```

---

## Task 12: Expanded TS models and a refresh-safe `ReportService`

**Files:**
- Modify: `frontend/src/app/report/report-entry.ts`
- Modify: `frontend/src/app/report/report.service.ts`
- Test: `frontend/src/app/report/report.service.spec.ts`

**Interfaces:**
- Consumes: `readPreference` / `writePreference` (Task 11); the JSON shape from
  Task 6 and Task 9.
- Produces:
  - `ReportEntry` interface (18 fields) and `IngestionStatus` interface — used by
    Tasks 13–19.
  - `ReportService` with signals `status`, `entries`, `errorMessage`,
    `retryCount`, `stale`, `lastLoadedAt`, `autoRefresh`, and methods
    `load()`, `refresh()`, `setAutoRefresh(on: boolean)`, `startPolling()`.

**The behaviour this task exists to get right:** today any non-503 error sets
`status='error'` and the template hides the table. Under a 5-second poll, one
blip would blank the page. So `load()` (initial) and `refresh()` (poll/manual)
must fail *differently*: initial failure shows the error screen; refresh failure
keeps the last good rows and raises `stale`.

Polling also pauses while the tab is hidden, and resumes with an immediate fetch
so a returning tab is not up to 5s out of date.

- [ ] **Step 1: Replace the TS models**

Replace `frontend/src/app/report/report-entry.ts`:

```ts
/**
 * One report row. The three PascalCase properties are the frozen legacy contract
 * (they drive the CSV); everything else is additive.
 *
 * Dates arrive as ISO strings ("2010-09-10"), not Date objects — Jackson emits
 * ISO because Spring Boot disables WRITE_DATES_AS_TIMESTAMPS by default.
 */
export interface ReportEntry {
  Client_Information: string;
  Product_Information: string;
  Total_Transaction_Amount: number;

  clientType: string;
  clientNumber: string;
  accountNumber: string;
  subaccountNumber: string;
  exchangeCode: string;
  productGroupCode: string;
  symbol: string;
  expirationDate: string;

  grossLong: number;
  grossShort: number;
  tradeCount: number;
  firstTransactionDate: string | null;
  lastTransactionDate: string | null;
  lastUpdatedAt: string | null;
  /** Keyed by currency code. Values are negative for debits — see the plan's constraints. */
  feesByCurrency: Record<string, number>;
}

/** Provenance of the source file. Run fields are null until an ingestion has happened. */
export interface IngestionStatus {
  configuredPath: string;
  fileExists: boolean;
  fileSizeBytes: number | null;
  fileLastModified: string | null;
  lastIngestAt: string | null;
  fingerprint: string | null;
  totalLines: number | null;
  published: number | null;
  skipped: number | null;
  errorCount: number | null;
}
```

- [ ] **Step 2: Write the failing tests**

Add to `frontend/src/app/report/report.service.spec.ts`. Keep every existing
test — they must continue to pass unchanged. Add a row factory and the new cases:

```ts
function row(overrides: Partial<ReportEntry> = {}): ReportEntry {
  return {
    Client_Information: 'CL432100020001',
    Product_Information: 'SGXFUNK20100910',
    Total_Transaction_Amount: 46,
    clientType: 'CL',
    clientNumber: '4321',
    accountNumber: '0002',
    subaccountNumber: '0001',
    exchangeCode: 'SGX',
    productGroupCode: 'FU',
    symbol: 'NK',
    expirationDate: '2010-09-10',
    grossLong: 46,
    grossShort: 0,
    tradeCount: 3,
    firstTransactionDate: '2010-08-19',
    lastTransactionDate: '2010-08-20',
    lastUpdatedAt: '2026-08-12T14:31:52Z',
    feesByCurrency: { USD: -0.9 },
    ...overrides,
  };
}

describe('ReportService refresh semantics', () => {
  it('keeps the previously loaded rows when a refresh fails', () => {
    // The regression this exists to prevent: with a 5s poll, one failed request
    // must not blank a table the user is reading.
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);
    expect(service.status()).toBe('ready');

    service.refresh();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Server Error' },
    );

    expect(service.status()).toBe('ready');
    expect(service.entries().length).toBe(1);
    expect(service.stale()).toBe(true);
    expect(service.errorMessage()).toBe('boom');
  });

  it('clears stale once a later refresh succeeds', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);
    service.refresh();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Server Error' },
    );
    expect(service.stale()).toBe(true);

    service.refresh();
    httpMock.expectOne('/api/v1/report').flush([row(), row({ symbol: 'N1' })]);

    expect(service.stale()).toBe(false);
    expect(service.errorMessage()).toBeNull();
    expect(service.entries().length).toBe(2);
  });

  it('shows the error screen when the INITIAL load fails', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'boom' },
      { status: 500, statusText: 'Server Error' },
    );

    expect(service.status()).toBe('error');
    expect(service.entries()).toEqual([]);
  });

  it('treats a 503 during refresh as stale rather than restarting the retry loop', () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);

    service.refresh();
    httpMock.expectOne('/api/v1/report').flush(
      { error: 'not ready' },
      { status: 503, statusText: 'Service Unavailable' },
    );

    expect(service.status()).toBe('ready');
    expect(service.stale()).toBe(true);
    // No pending retry timer should have been scheduled.
    httpMock.verify();
  });

  it('records lastLoadedAt on success', () => {
    expect(service.lastLoadedAt()).toBeNull();
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);
    expect(service.lastLoadedAt()).not.toBeNull();
  });
});

describe('ReportService polling', () => {
  it('polls every 5s once started', async () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);

    service.startPolling();
    await vi.advanceTimersByTimeAsync(5000);
    httpMock.expectOne('/api/v1/report').flush([row()]);
    await vi.advanceTimersByTimeAsync(5000);
    httpMock.expectOne('/api/v1/report').flush([row()]);
  });

  it('does not poll while auto-refresh is off', async () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);

    service.setAutoRefresh(false);
    service.startPolling();
    await vi.advanceTimersByTimeAsync(15000);

    // No outstanding requests: httpMock.verify() in afterEach would fail otherwise.
    httpMock.verify();
  });

  it('stops polling while the tab is hidden and refetches immediately on return', async () => {
    service.load();
    httpMock.expectOne('/api/v1/report').flush([row()]);
    service.startPolling();

    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden');
    document.dispatchEvent(new Event('visibilitychange'));
    await vi.advanceTimersByTimeAsync(15000);
    httpMock.verify(); // nothing fired while hidden

    vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
    document.dispatchEvent(new Event('visibilitychange'));
    // Resuming refetches at once rather than waiting out the interval.
    httpMock.expectOne('/api/v1/report').flush([row()]);

    vi.restoreAllMocks();
  });

  it('persists the auto-refresh choice', () => {
    service.setAutoRefresh(false);
    expect(localStorage.getItem('pfm.autoRefresh')).toBe('false');
  });
});
```

Add `import { ReportEntry } from './report-entry';` if absent, and
`localStorage.clear()` to the existing `beforeEach`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cd frontend && npx vitest run src/app/report/report.service.spec.ts`
Expected: FAIL — `service.stale is not a function`.

- [ ] **Step 4: Rewrite `ReportService`**

Replace `frontend/src/app/report/report.service.ts`:

```ts
import { Injectable, inject, signal } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { timer } from 'rxjs';
import { ReportEntry } from './report-entry';
import { readPreference, writePreference } from '../shared/local-preference';

const RETRY_DELAY_MS = 3000;
const POLL_INTERVAL_MS = 5000;
const AUTO_REFRESH_KEY = 'pfm.autoRefresh';

export type ReportStatus = 'loading' | 'ready' | 'error';

@Injectable({ providedIn: 'root' })
export class ReportService {
  private readonly http = inject(HttpClient);

  private readonly _status = signal<ReportStatus>('loading');
  private readonly _entries = signal<ReportEntry[]>([]);
  private readonly _errorMessage = signal<string | null>(null);
  private readonly _retryCount = signal<number>(0);
  private readonly _stale = signal<boolean>(false);
  private readonly _lastLoadedAt = signal<Date | null>(null);
  private readonly _autoRefresh = signal<boolean>(readPreference(AUTO_REFRESH_KEY, true));

  readonly status = this._status.asReadonly();
  readonly entries = this._entries.asReadonly();
  readonly errorMessage = this._errorMessage.asReadonly();
  readonly retryCount = this._retryCount.asReadonly();
  readonly stale = this._stale.asReadonly();
  readonly lastLoadedAt = this._lastLoadedAt.asReadonly();
  readonly autoRefresh = this._autoRefresh.asReadonly();

  private pollTimer: ReturnType<typeof setInterval> | null = null;

  constructor() {
    document.addEventListener('visibilitychange', () => this.syncPolling(true));
  }

  /** Initial load: a failure here is fatal to the view and shows the error screen. */
  load(): void {
    this._status.set('loading');
    this._errorMessage.set(null);
    this._retryCount.set(0);
    this._stale.set(false);
    this.fetch(true);
  }

  /** Poll or manual refresh: a failure here must preserve whatever is on screen. */
  refresh(): void {
    this.fetch(false);
  }

  startPolling(): void {
    this.syncPolling(false);
  }

  setAutoRefresh(on: boolean): void {
    this._autoRefresh.set(on);
    writePreference(AUTO_REFRESH_KEY, on);
    this.syncPolling(false);
  }

  /**
   * @param fetchOnResume refetch immediately when polling (re)starts. True for a
   *        tab becoming visible again — its data may be up to an interval stale —
   *        and false when the caller has just loaded, to avoid a double request.
   */
  private syncPolling(fetchOnResume: boolean): void {
    const shouldPoll = this._autoRefresh() && document.visibilityState !== 'hidden';

    if (shouldPoll && this.pollTimer === null) {
      this.pollTimer = setInterval(() => this.refresh(), POLL_INTERVAL_MS);
      if (fetchOnResume) {
        this.refresh();
      }
      return;
    }
    if (!shouldPoll && this.pollTimer !== null) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
  }

  private fetch(initial: boolean): void {
    this.http.get<ReportEntry[]>('/api/v1/report').subscribe({
      next: (entries) => {
        this._entries.set(entries);
        this._status.set('ready');
        this._stale.set(false);
        this._errorMessage.set(null);
        this._lastLoadedAt.set(new Date());
      },
      error: (err: HttpErrorResponse) => {
        // 503 means "Kafka Streams still starting". Worth waiting out on first
        // load; on a refresh we already have data, so just mark it stale.
        if (err.status === 503 && initial) {
          timer(RETRY_DELAY_MS).subscribe(() => {
            this._retryCount.update((count) => count + 1);
            this.fetch(true);
          });
          return;
        }

        const message =
          err.error?.error ?? err.message ?? 'Unable to reach processing-service.';

        if (initial || this._status() !== 'ready') {
          this._status.set('error');
          this._errorMessage.set(message);
          return;
        }

        // Data is already on screen: keep it and flag it rather than blanking the view.
        this._stale.set(true);
        this._errorMessage.set(message);
      },
    });
  }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd frontend && npx vitest run src/app/report/report.service.spec.ts`
Expected: PASS — the new tests plus all pre-existing 503-retry tests.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/app/report/report-entry.ts \
        frontend/src/app/report/report.service.ts \
        frontend/src/app/report/report.service.spec.ts
git commit -m "feat(frontend): distinguish load from refresh failures and add 5s polling"
```

---

## Task 13: Column definitions and persisted visibility

**Files:**
- Create: `frontend/src/app/report/report-columns.ts`
- Create: `frontend/src/app/report/column-preferences.ts`
- Test: `frontend/src/app/report/column-preferences.spec.ts`

**Interfaces:**
- Consumes: `ReportEntry` (Task 12), `readPreference`/`writePreference` (Task 11).
- Produces:
  - `ColumnDef` interface, `REPORT_COLUMNS: readonly ColumnDef[]`,
    `DEFAULT_VISIBLE_COLUMN_IDS: readonly string[]`.
  - `ColumnPreferences` (injectable) with `visibleIds: Signal<readonly string[]>`,
    `visibleColumns: Signal<readonly ColumnDef[]>`, `toggle(id)`, `reset()`,
    `isVisible(id): boolean`.
- Tasks 16, 17 render from `visibleColumns()`.

One declarative array drives the header, the cells and the picker, so a header
can never desynchronise from its cells and adding a column is a one-line change.
Eight of the seventeen columns are visible by default.

- [ ] **Step 1: Create the column definitions**

Create `frontend/src/app/report/report-columns.ts`:

```ts
import { ReportEntry } from './report-entry';

export type ColumnGroup = 'Client' | 'Product' | 'Position' | 'Activity' | 'Legacy';

/** How a cell is drawn. `divergingBar` and `expiry` get bespoke templates. */
export type ColumnRender = 'text' | 'date' | 'number' | 'divergingBar' | 'expiry';

export interface ColumnDef {
  id: string;
  label: string;
  group: ColumnGroup;
  /** Right-align numbers so digits line up; left-align identifiers. */
  align: 'left' | 'right';
  /** Applies tabular-nums so columns of figures align vertically. */
  numeric: boolean;
  defaultVisible: boolean;
  render: ColumnRender;
  /** Value used for sorting and for the global search. */
  sortValue: (entry: ReportEntry) => string | number;
}

export const REPORT_COLUMNS: readonly ColumnDef[] = [
  // --- Client ---
  { id: 'clientNumber', label: 'Client', group: 'Client', align: 'left', numeric: false,
    defaultVisible: true, render: 'text', sortValue: (e) => e.clientNumber },
  { id: 'accountNumber', label: 'Account', group: 'Client', align: 'left', numeric: false,
    defaultVisible: true, render: 'text', sortValue: (e) => e.accountNumber },
  { id: 'clientType', label: 'Client type', group: 'Client', align: 'left', numeric: false,
    defaultVisible: false, render: 'text', sortValue: (e) => e.clientType },
  { id: 'subaccountNumber', label: 'Subaccount', group: 'Client', align: 'left', numeric: false,
    defaultVisible: false, render: 'text', sortValue: (e) => e.subaccountNumber },

  // --- Product ---
  { id: 'symbol', label: 'Symbol', group: 'Product', align: 'left', numeric: false,
    defaultVisible: true, render: 'text', sortValue: (e) => e.symbol },
  { id: 'expirationDate', label: 'Expiry', group: 'Product', align: 'left', numeric: false,
    defaultVisible: true, render: 'expiry', sortValue: (e) => e.expirationDate },
  { id: 'exchangeCode', label: 'Exchange', group: 'Product', align: 'left', numeric: false,
    defaultVisible: false, render: 'text', sortValue: (e) => e.exchangeCode },
  { id: 'productGroupCode', label: 'Group', group: 'Product', align: 'left', numeric: false,
    defaultVisible: false, render: 'text', sortValue: (e) => e.productGroupCode },

  // --- Position ---
  { id: 'netQuantity', label: 'Net', group: 'Position', align: 'right', numeric: true,
    defaultVisible: true, render: 'divergingBar', sortValue: (e) => e.Total_Transaction_Amount },
  { id: 'grossLong', label: 'Gross long', group: 'Position', align: 'right', numeric: true,
    defaultVisible: true, render: 'number', sortValue: (e) => e.grossLong },
  { id: 'grossShort', label: 'Gross short', group: 'Position', align: 'right', numeric: true,
    defaultVisible: true, render: 'number', sortValue: (e) => e.grossShort },
  { id: 'tradeCount', label: 'Trades', group: 'Position', align: 'right', numeric: true,
    defaultVisible: true, render: 'number', sortValue: (e) => e.tradeCount },

  // --- Activity ---
  { id: 'firstTransactionDate', label: 'First trade', group: 'Activity', align: 'left',
    numeric: false, defaultVisible: false, render: 'date',
    sortValue: (e) => e.firstTransactionDate ?? '' },
  { id: 'lastTransactionDate', label: 'Last trade', group: 'Activity', align: 'left',
    numeric: false, defaultVisible: false, render: 'date',
    sortValue: (e) => e.lastTransactionDate ?? '' },
  { id: 'lastUpdatedAt', label: 'Updated', group: 'Activity', align: 'left', numeric: false,
    defaultVisible: false, render: 'date', sortValue: (e) => e.lastUpdatedAt ?? '' },

  // --- Legacy: the concatenated strings, available but off by default ---
  { id: 'Client_Information', label: 'Client_Information', group: 'Legacy', align: 'left',
    numeric: false, defaultVisible: false, render: 'text', sortValue: (e) => e.Client_Information },
  { id: 'Product_Information', label: 'Product_Information', group: 'Legacy', align: 'left',
    numeric: false, defaultVisible: false, render: 'text',
    sortValue: (e) => e.Product_Information },
];

export const DEFAULT_VISIBLE_COLUMN_IDS: readonly string[] = REPORT_COLUMNS.filter(
  (column) => column.defaultVisible,
).map((column) => column.id);

export const COLUMN_GROUPS: readonly ColumnGroup[] = [
  'Client',
  'Product',
  'Position',
  'Activity',
  'Legacy',
];
```

- [ ] **Step 2: Write the failing test**

Create `frontend/src/app/report/column-preferences.spec.ts`:

```ts
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { ColumnPreferences } from './column-preferences';
import { DEFAULT_VISIBLE_COLUMN_IDS, REPORT_COLUMNS } from './report-columns';

describe('ColumnPreferences', () => {
  beforeEach(() => {
    localStorage.clear();
    TestBed.resetTestingModule();
  });

  it('starts with the eight default columns', () => {
    const prefs = TestBed.inject(ColumnPreferences);

    expect(prefs.visibleIds()).toEqual(DEFAULT_VISIBLE_COLUMN_IDS);
    expect(prefs.visibleIds().length).toBe(8);
  });

  it('exposes seventeen definitions in total', () => {
    expect(REPORT_COLUMNS.length).toBe(17);
  });

  it('preserves declaration order regardless of toggle order', () => {
    const prefs = TestBed.inject(ColumnPreferences);

    prefs.toggle('Client_Information'); // last in declaration order
    prefs.toggle('clientType');         // third

    const ids = prefs.visibleColumns().map((c) => c.id);
    const declarationOrder = REPORT_COLUMNS.map((c) => c.id).filter((id) => ids.includes(id));
    expect(ids).toEqual(declarationOrder);
  });

  it('toggles a column off and on', () => {
    const prefs = TestBed.inject(ColumnPreferences);

    prefs.toggle('tradeCount');
    expect(prefs.isVisible('tradeCount')).toBe(false);

    prefs.toggle('tradeCount');
    expect(prefs.isVisible('tradeCount')).toBe(true);
  });

  it('persists across instances', () => {
    TestBed.inject(ColumnPreferences).toggle('grossShort');

    TestBed.resetTestingModule();
    expect(TestBed.inject(ColumnPreferences).isVisible('grossShort')).toBe(false);
  });

  it('falls back to defaults when stored ids are unknown', () => {
    // A future release renaming a column must not leave anyone with a broken table.
    localStorage.setItem('pfm.visibleColumns', JSON.stringify(['nope', 'alsoNope']));

    expect(TestBed.inject(ColumnPreferences).visibleIds()).toEqual(DEFAULT_VISIBLE_COLUMN_IDS);
  });

  it('drops unknown ids but keeps recognised ones', () => {
    localStorage.setItem('pfm.visibleColumns', JSON.stringify(['symbol', 'nope']));

    expect(TestBed.inject(ColumnPreferences).visibleIds()).toEqual(['symbol']);
  });

  it('falls back to defaults when the stored value is not an array', () => {
    localStorage.setItem('pfm.visibleColumns', JSON.stringify({ symbol: true }));

    expect(TestBed.inject(ColumnPreferences).visibleIds()).toEqual(DEFAULT_VISIBLE_COLUMN_IDS);
  });

  it('reset restores the defaults', () => {
    const prefs = TestBed.inject(ColumnPreferences);
    prefs.toggle('symbol');
    prefs.toggle('clientType');

    prefs.reset();

    expect(prefs.visibleIds()).toEqual(DEFAULT_VISIBLE_COLUMN_IDS);
  });
});
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd frontend && npx vitest run src/app/report/column-preferences.spec.ts`
Expected: FAIL — cannot resolve `./column-preferences`.

- [ ] **Step 4: Implement `ColumnPreferences`**

Create `frontend/src/app/report/column-preferences.ts`:

```ts
import { Injectable, computed, signal } from '@angular/core';
import { readPreference, writePreference } from '../shared/local-preference';
import { ColumnDef, DEFAULT_VISIBLE_COLUMN_IDS, REPORT_COLUMNS } from './report-columns';

const STORAGE_KEY = 'pfm.visibleColumns';

@Injectable({ providedIn: 'root' })
export class ColumnPreferences {
  private readonly _visibleIds = signal<readonly string[]>(restore());

  readonly visibleIds = this._visibleIds.asReadonly();

  /**
   * Always in declaration order, never in the order the user toggled things —
   * otherwise columns would jump around as they are switched on.
   */
  readonly visibleColumns = computed<readonly ColumnDef[]>(() => {
    const visible = new Set(this._visibleIds());
    return REPORT_COLUMNS.filter((column) => visible.has(column.id));
  });

  isVisible(id: string): boolean {
    return this._visibleIds().includes(id);
  }

  toggle(id: string): void {
    const next = this.isVisible(id)
      ? this._visibleIds().filter((visibleId) => visibleId !== id)
      : [...this._visibleIds(), id];
    this._visibleIds.set(next);
    writePreference(STORAGE_KEY, next);
  }

  reset(): void {
    this._visibleIds.set([...DEFAULT_VISIBLE_COLUMN_IDS]);
    writePreference(STORAGE_KEY, DEFAULT_VISIBLE_COLUMN_IDS);
  }
}

/**
 * Stored ids are filtered against the current definitions, so renaming or removing
 * a column in a future release cannot strand a user with an empty table.
 */
function restore(): readonly string[] {
  const stored = readPreference<unknown>(STORAGE_KEY, null);
  if (!Array.isArray(stored)) {
    return [...DEFAULT_VISIBLE_COLUMN_IDS];
  }
  const known = new Set(REPORT_COLUMNS.map((column) => column.id));
  const filtered = stored.filter((id): id is string => typeof id === 'string' && known.has(id));
  return filtered.length > 0 ? filtered : [...DEFAULT_VISIBLE_COLUMN_IDS];
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd frontend && npx vitest run src/app/report/column-preferences.spec.ts`
Expected: PASS — 9 tests.

- [ ] **Step 6: Commit**

```bash
git add frontend/src/app/report/report-columns.ts \
        frontend/src/app/report/column-preferences.ts \
        frontend/src/app/report/column-preferences.spec.ts
git commit -m "feat(frontend): add column definitions and persisted column visibility"
```

---

## Task 14: Filtering and sorting

**Files:**
- Create: `frontend/src/app/report/report-filters.ts`
- Test: `frontend/src/app/report/report-filters.spec.ts`

**Interfaces:**
- Consumes: `ReportEntry` (Task 12), `REPORT_COLUMNS` (Task 13), `ReportService`
  (Task 12).
- Produces:
  - `FilterCriteria` interface and the pure function
    `filterAndSort(entries, criteria): ReportEntry[]`.
  - `ReportFilters` (injectable) with signals `client`, `account`, `product`,
    `search`, `sortColumnId`, `sortDirection`; computed `clientOptions`,
    `accountOptions`, `productOptions`, `rows`, `activeFilterCount`,
    `totalCount`; methods `setClient`, `setAccount`, `setProduct`, `setSearch`,
    `toggleSort(columnId)`, `clearAll()`.
- Tasks 16, 17, 18 consume `rows()` and the option lists.

The matching logic is a **pure function** so it can be tested without Angular
wiring; the service only holds signals and feeds `reportService.entries()` in.
Filters compose with AND. Options are derived from the response, so they hold 3
values today and 300 later with no code change.

- [ ] **Step 1: Write the failing test**

Create `frontend/src/app/report/report-filters.spec.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { FilterCriteria, filterAndSort } from './report-filters';
import { ReportEntry } from './report-entry';

function row(overrides: Partial<ReportEntry> = {}): ReportEntry {
  return {
    Client_Information: 'CL432100020001',
    Product_Information: 'SGXFUNK20100910',
    Total_Transaction_Amount: 46,
    clientType: 'CL',
    clientNumber: '4321',
    accountNumber: '0002',
    subaccountNumber: '0001',
    exchangeCode: 'SGX',
    productGroupCode: 'FU',
    symbol: 'NK',
    expirationDate: '2010-09-10',
    grossLong: 46,
    grossShort: 0,
    tradeCount: 3,
    firstTransactionDate: '2010-08-19',
    lastTransactionDate: '2010-08-20',
    lastUpdatedAt: '2026-08-12T14:31:52Z',
    feesByCurrency: { USD: -0.9 },
    ...overrides,
  };
}

const NO_FILTERS: FilterCriteria = {
  client: '',
  account: '',
  product: '',
  search: '',
  sortColumnId: null,
  sortDirection: 'asc',
};

describe('filterAndSort', () => {
  it('returns everything when no filter is set', () => {
    const rows = [row(), row({ clientNumber: '1234' })];

    expect(filterAndSort(rows, NO_FILTERS).length).toBe(2);
  });

  it('filters by client information', () => {
    const rows = [row(), row({ Client_Information: 'CL123400030001' })];

    const result = filterAndSort(rows, { ...NO_FILTERS, client: 'CL123400030001' });

    expect(result.length).toBe(1);
    expect(result[0].Client_Information).toBe('CL123400030001');
  });

  it('filters by account number', () => {
    const rows = [row({ accountNumber: '0002' }), row({ accountNumber: '0003' })];

    const result = filterAndSort(rows, { ...NO_FILTERS, account: '0003' });

    expect(result.length).toBe(1);
    expect(result[0].accountNumber).toBe('0003');
  });

  it('filters by product information', () => {
    const rows = [row(), row({ Product_Information: 'CMEFUNK.20100910' })];

    const result = filterAndSort(rows, { ...NO_FILTERS, product: 'CMEFUNK.20100910' });

    expect(result.length).toBe(1);
  });

  it('composes filters with AND', () => {
    const rows = [
      row({ accountNumber: '0002', Product_Information: 'SGXFUNK20100910' }),
      row({ accountNumber: '0003', Product_Information: 'SGXFUNK20100910' }),
      row({ accountNumber: '0003', Product_Information: 'CMEFUNK.20100910' }),
    ];

    const result = filterAndSort(rows, {
      ...NO_FILTERS,
      account: '0003',
      product: 'CMEFUNK.20100910',
    });

    expect(result.length).toBe(1);
  });

  it('searches case-insensitively across fields', () => {
    const rows = [row({ symbol: 'NK' }), row({ symbol: 'N1' })];

    expect(filterAndSort(rows, { ...NO_FILTERS, search: 'nk' }).length).toBe(1);
  });

  it('matches a numeric value via search', () => {
    const rows = [row({ Total_Transaction_Amount: 46 }), row({ Total_Transaction_Amount: -215 })];

    expect(filterAndSort(rows, { ...NO_FILTERS, search: '-215' }).length).toBe(1);
  });

  it('returns an empty array when filters exclude everything', () => {
    expect(filterAndSort([row()], { ...NO_FILTERS, account: 'nope' })).toEqual([]);
  });

  it('sorts ascending by a string column', () => {
    const rows = [row({ symbol: 'NK' }), row({ symbol: 'N1' })];

    const result = filterAndSort(rows, { ...NO_FILTERS, sortColumnId: 'symbol' });

    expect(result.map((r) => r.symbol)).toEqual(['N1', 'NK']);
  });

  it('sorts descending when direction is desc', () => {
    const rows = [row({ symbol: 'N1' }), row({ symbol: 'NK' })];

    const result = filterAndSort(rows, {
      ...NO_FILTERS,
      sortColumnId: 'symbol',
      sortDirection: 'desc',
    });

    expect(result.map((r) => r.symbol)).toEqual(['NK', 'N1']);
  });

  it('sorts numerically, not lexicographically', () => {
    const rows = [
      row({ Total_Transaction_Amount: 46 }),
      row({ Total_Transaction_Amount: -215 }),
      row({ Total_Transaction_Amount: 285 }),
    ];

    const result = filterAndSort(rows, { ...NO_FILTERS, sortColumnId: 'netQuantity' });

    // Lexicographic order would give -215, 285, 46.
    expect(result.map((r) => r.Total_Transaction_Amount)).toEqual([-215, 46, 285]);
  });

  it('preserves the server order when no sort column is set', () => {
    const rows = [row({ symbol: 'NK' }), row({ symbol: 'N1' })];

    expect(filterAndSort(rows, NO_FILTERS).map((r) => r.symbol)).toEqual(['NK', 'N1']);
  });

  it('does not mutate the input array', () => {
    const rows = [row({ symbol: 'NK' }), row({ symbol: 'N1' })];

    filterAndSort(rows, { ...NO_FILTERS, sortColumnId: 'symbol' });

    expect(rows.map((r) => r.symbol)).toEqual(['NK', 'N1']);
  });

  it('ignores an unknown sort column rather than throwing', () => {
    expect(filterAndSort([row()], { ...NO_FILTERS, sortColumnId: 'nope' }).length).toBe(1);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd frontend && npx vitest run src/app/report/report-filters.spec.ts`
Expected: FAIL — cannot resolve `./report-filters`.

- [ ] **Step 3: Implement**

Create `frontend/src/app/report/report-filters.ts`:

```ts
import { Injectable, computed, inject, signal } from '@angular/core';
import { ReportEntry } from './report-entry';
import { REPORT_COLUMNS } from './report-columns';
import { ReportService } from './report.service';

export type SortDirection = 'asc' | 'desc';

export interface FilterCriteria {
  /** '' means "all" for each dimension. */
  client: string;
  account: string;
  product: string;
  search: string;
  sortColumnId: string | null;
  sortDirection: SortDirection;
}

/**
 * Pure so it can be tested without Angular. Filters compose with AND; an empty
 * dimension value means "all". A null sortColumnId preserves the server's order,
 * which is already sorted by client then product.
 */
export function filterAndSort(
  entries: readonly ReportEntry[],
  criteria: FilterCriteria,
): ReportEntry[] {
  const search = criteria.search.trim().toLowerCase();

  const filtered = entries.filter((entry) => {
    if (criteria.client && entry.Client_Information !== criteria.client) return false;
    if (criteria.account && entry.accountNumber !== criteria.account) return false;
    if (criteria.product && entry.Product_Information !== criteria.product) return false;
    if (!search) return true;
    return REPORT_COLUMNS.some((column) =>
      String(column.sortValue(entry)).toLowerCase().includes(search),
    );
  });

  const column = REPORT_COLUMNS.find((candidate) => candidate.id === criteria.sortColumnId);
  if (!column) {
    return filtered;
  }

  const direction = criteria.sortDirection === 'desc' ? -1 : 1;
  // Copy first: sort() mutates, and the input is the service's signal value.
  return [...filtered].sort((left, right) => {
    const a = column.sortValue(left);
    const b = column.sortValue(right);
    if (typeof a === 'number' && typeof b === 'number') {
      return (a - b) * direction;
    }
    return String(a).localeCompare(String(b)) * direction;
  });
}

@Injectable({ providedIn: 'root' })
export class ReportFilters {
  private readonly reportService = inject(ReportService);

  readonly client = signal('');
  readonly account = signal('');
  readonly product = signal('');
  readonly search = signal('');
  readonly sortColumnId = signal<string | null>(null);
  readonly sortDirection = signal<SortDirection>('asc');

  /** Options come from the data, so cardinality is whatever the response contains. */
  readonly clientOptions = computed(() => distinct(this.reportService.entries(), (e) => e.Client_Information));
  readonly accountOptions = computed(() => distinct(this.reportService.entries(), (e) => e.accountNumber));
  readonly productOptions = computed(() => distinct(this.reportService.entries(), (e) => e.Product_Information));

  readonly rows = computed(() =>
    filterAndSort(this.reportService.entries(), {
      client: this.client(),
      account: this.account(),
      product: this.product(),
      search: this.search(),
      sortColumnId: this.sortColumnId(),
      sortDirection: this.sortDirection(),
    }),
  );

  readonly totalCount = computed(() => this.reportService.entries().length);

  readonly activeFilterCount = computed(
    () =>
      [this.client(), this.account(), this.product(), this.search().trim()].filter(
        (value) => value !== '',
      ).length,
  );

  setClient(value: string): void {
    this.client.set(value);
  }

  setAccount(value: string): void {
    this.account.set(value);
  }

  setProduct(value: string): void {
    this.product.set(value);
  }

  setSearch(value: string): void {
    this.search.set(value);
  }

  /** First click sorts ascending; clicking the active column flips direction. */
  toggleSort(columnId: string): void {
    if (this.sortColumnId() === columnId) {
      this.sortDirection.update((direction) => (direction === 'asc' ? 'desc' : 'asc'));
      return;
    }
    this.sortColumnId.set(columnId);
    this.sortDirection.set('asc');
  }

  clearAll(): void {
    this.client.set('');
    this.account.set('');
    this.product.set('');
    this.search.set('');
  }
}

function distinct(entries: readonly ReportEntry[], select: (entry: ReportEntry) => string): string[] {
  return [...new Set(entries.map(select))].sort((a, b) => a.localeCompare(b));
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd frontend && npx vitest run src/app/report/report-filters.spec.ts`
Expected: PASS — 14 tests.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/app/report/report-filters.ts \
        frontend/src/app/report/report-filters.spec.ts
git commit -m "feat(frontend): add client-side filtering and sorting for the report"
```

---

## Task 15: Source-file panel

**Files:**
- Create: `frontend/src/app/report/ingestion-status.service.ts`
- Create: `frontend/src/app/report/source-file-panel.ts`
- Create: `frontend/src/app/report/format.ts`
- Test: `frontend/src/app/report/ingestion-status.service.spec.ts`
- Test: `frontend/src/app/report/format.spec.ts`

**Interfaces:**
- Consumes: `IngestionStatus` (Task 12); `GET /api/v1/ingest/status` (Tasks 9–10).
- Produces:
  - `IngestionStatusService` with `status: Signal<IngestionStatus | null>`,
    `available: Signal<boolean>`, `load()`.
  - `formatBytes(n)`, `formatDateTime(iso)`, `formatRelative(iso, now)` in
    `format.ts` — reused by Tasks 16 and 17.
  - `SourceFilePanel` standalone component, selector `app-source-file-panel`.

The status endpoint failing must **not** affect the report — the panel degrades to
"unavailable" on its own.

- [ ] **Step 1: Write the failing format test**

Create `frontend/src/app/report/format.spec.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { formatBytes, formatDateTime, formatRelative } from './format';

describe('formatBytes', () => {
  it('formats bytes, KB and MB', () => {
    expect(formatBytes(512)).toBe('512 B');
    expect(formatBytes(127624)).toBe('124.6 KB');
    expect(formatBytes(5 * 1024 * 1024)).toBe('5.0 MB');
  });

  it('renders an em dash for null', () => {
    expect(formatBytes(null)).toBe('—');
  });

  it('handles zero without dividing', () => {
    expect(formatBytes(0)).toBe('0 B');
  });
});

describe('formatDateTime', () => {
  it('renders an em dash for null', () => {
    expect(formatDateTime(null)).toBe('—');
  });

  it('includes the date and time', () => {
    const formatted = formatDateTime('2026-08-12T14:31:52Z');
    expect(formatted).toContain('2026');
  });
});

describe('formatRelative', () => {
  const now = new Date('2026-08-12T14:35:00Z');

  it('reports seconds under a minute', () => {
    expect(formatRelative('2026-08-12T14:34:30Z', now)).toBe('30s ago');
  });

  it('reports whole minutes', () => {
    expect(formatRelative('2026-08-12T14:32:00Z', now)).toBe('3m ago');
  });

  it('reports hours', () => {
    expect(formatRelative('2026-08-12T11:35:00Z', now)).toBe('3h ago');
  });

  it('says just now for the current instant', () => {
    expect(formatRelative('2026-08-12T14:35:00Z', now)).toBe('just now');
  });

  it('renders an em dash for null', () => {
    expect(formatRelative(null, now)).toBe('—');
  });
});
```

- [ ] **Step 2: Implement the formatters**

Create `frontend/src/app/report/format.ts`:

```ts
const EM_DASH = '—';

export function formatBytes(bytes: number | null): string {
  if (bytes === null) return EM_DASH;
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export function formatDateTime(iso: string | null): string {
  if (!iso) return EM_DASH;
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return EM_DASH;
  return date.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

/** @param now injected so tests are deterministic. */
export function formatRelative(iso: string | null, now: Date = new Date()): string {
  if (!iso) return EM_DASH;
  const then = new Date(iso);
  if (Number.isNaN(then.getTime())) return EM_DASH;

  const seconds = Math.floor((now.getTime() - then.getTime()) / 1000);
  if (seconds <= 0) return 'just now';
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}
```

- [ ] **Step 3: Write the failing status-service test**

Create `frontend/src/app/report/ingestion-status.service.spec.ts`:

```ts
import { TestBed } from '@angular/core/testing';
import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { IngestionStatusService } from './ingestion-status.service';
import { IngestionStatus } from './report-entry';

const SAMPLE: IngestionStatus = {
  configuredPath: 'sample-data/Input.txt',
  fileExists: true,
  fileSizeBytes: 127624,
  fileLastModified: '2026-08-12T09:14:00Z',
  lastIngestAt: '2026-08-12T14:31:52Z',
  fingerprint: 'fp-1',
  totalLines: 717,
  published: 717,
  skipped: 0,
  errorCount: 0,
};

describe('IngestionStatusService', () => {
  let service: IngestionStatusService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    service = TestBed.inject(IngestionStatusService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => httpMock.verify());

  it('starts with no status', () => {
    expect(service.status()).toBeNull();
    expect(service.available()).toBe(false);
  });

  it('loads the status', () => {
    service.load();
    httpMock.expectOne('/api/v1/ingest/status').flush(SAMPLE);

    expect(service.status()).toEqual(SAMPLE);
    expect(service.available()).toBe(true);
  });

  it('degrades to unavailable on failure without throwing', () => {
    // The report must not be affected by the status endpoint being down.
    service.load();
    httpMock.expectOne('/api/v1/ingest/status').flush(
      { error: 'nope' },
      { status: 500, statusText: 'Server Error' },
    );

    expect(service.status()).toBeNull();
    expect(service.available()).toBe(false);
  });
});
```

- [ ] **Step 4: Run both tests to verify they fail**

Run: `cd frontend && npx vitest run src/app/report/format.spec.ts src/app/report/ingestion-status.service.spec.ts`
Expected: FAIL — cannot resolve `./ingestion-status.service`.

- [ ] **Step 5: Implement the status service**

Create `frontend/src/app/report/ingestion-status.service.ts`:

```ts
import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { IngestionStatus } from './report-entry';

@Injectable({ providedIn: 'root' })
export class IngestionStatusService {
  private readonly http = inject(HttpClient);
  private readonly _status = signal<IngestionStatus | null>(null);

  readonly status = this._status.asReadonly();
  readonly available = computed(() => this._status() !== null);

  load(): void {
    this.http.get<IngestionStatus>('/api/v1/ingest/status').subscribe({
      next: (status) => this._status.set(status),
      // Provenance is supplementary; losing it must never break the report view.
      error: () => this._status.set(null),
    });
  }
}
```

- [ ] **Step 6: Implement the panel**

Create `frontend/src/app/report/source-file-panel.ts`:

```ts
import { Component, inject } from '@angular/core';
import { IngestionStatusService } from './ingestion-status.service';
import { formatBytes, formatDateTime, formatRelative } from './format';

@Component({
  selector: 'app-source-file-panel',
  template: `
    <section
      class="rounded-lg border border-rule bg-surface-1 p-4"
      aria-labelledby="source-file-heading"
    >
      <h2 id="source-file-heading" class="text-xs font-semibold uppercase tracking-wide text-ink-muted">
        Source file
      </h2>

      @if (statusService.status(); as status) {
        <p class="mt-2 font-mono text-sm text-ink-primary" data-testid="configured-path">
          {{ status.configuredPath }}
        </p>

        @if (status.fileExists) {
          <p class="mt-1 text-sm text-ink-secondary">
            {{ bytes(status.fileSizeBytes) }} · modified {{ dateTime(status.fileLastModified) }}
          </p>
        } @else {
          <p class="mt-1 text-sm text-status-critical" data-testid="file-missing">
            ⚠ File not found at this path
          </p>
        }

        @if (status.lastIngestAt) {
          <dl class="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm sm:grid-cols-4">
            <div>
              <dt class="text-ink-muted">Ingested</dt>
              <dd class="text-ink-primary" data-testid="ingested-at">
                {{ dateTime(status.lastIngestAt) }} ({{ relative(status.lastIngestAt) }})
              </dd>
            </div>
            <div>
              <dt class="text-ink-muted">Published</dt>
              <dd class="tabular text-ink-primary">{{ status.published }}</dd>
            </div>
            <div>
              <dt class="text-ink-muted">Skipped</dt>
              <dd class="tabular text-ink-primary">{{ status.skipped }}</dd>
            </div>
            <div>
              <dt class="text-ink-muted">Failed</dt>
              <dd
                class="tabular"
                [class.text-status-critical]="(status.errorCount ?? 0) > 0"
                [class.text-ink-primary]="(status.errorCount ?? 0) === 0"
              >
                {{ status.errorCount }}
              </dd>
            </div>
          </dl>
        } @else {
          <p class="mt-3 text-sm text-ink-secondary" data-testid="not-ingested">
            Not yet ingested.
          </p>
        }
      } @else {
        <p class="mt-2 text-sm text-ink-secondary" data-testid="status-unavailable">
          File details unavailable.
        </p>
      }
    </section>
  `,
})
export class SourceFilePanel {
  protected readonly statusService = inject(IngestionStatusService);

  protected bytes = formatBytes;
  protected dateTime = formatDateTime;
  protected relative = (iso: string | null) => formatRelative(iso);
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cd frontend && npx vitest run src/app/report/format.spec.ts src/app/report/ingestion-status.service.spec.ts`
Expected: PASS — 12 tests.

- [ ] **Step 8: Commit**

```bash
git add frontend/src/app/report/format.ts frontend/src/app/report/format.spec.ts \
        frontend/src/app/report/ingestion-status.service.ts \
        frontend/src/app/report/ingestion-status.service.spec.ts \
        frontend/src/app/report/source-file-panel.ts
git commit -m "feat(frontend): add the source-file provenance panel"
```

---

## Task 16: The table — diverging bar and trade-date-relative expiry

**Files:**
- Create: `frontend/src/app/report/cell-view.ts`
- Create: `frontend/src/app/report/report-table.ts`
- Create: `frontend/src/app/report/report-table.html`
- Test: `frontend/src/app/report/cell-view.spec.ts`
- Test: `frontend/src/app/report/report-table.spec.ts`

**Interfaces:**
- Consumes: `ColumnDef`, `ColumnPreferences` (Task 13); `ReportFilters` (Task 14);
  `formatDateTime` (Task 15).
- Produces:
  - `expiryBadge(expirationDate, lastTransactionDate): ExpiryBadge` and
    `barGeometry(value, maxAbs): BarGeometry` in `cell-view.ts`.
  - `ReportTable` standalone component, selector `app-report-table`.

**Two things this task must get right:**

1. **Expiry is measured against the row's last trade date, not wall clock.** The
   sample data expires `2010-09-10`; against today every row would be a red
   "expired" badge conveying nothing. Against the trade date (`2010-08-20`) it is
   21 days — informative. Labels say **"as of trade date"** so historical data
   cannot be misread as live contract status.
2. **Colour is never the only channel.** The signed number is always rendered as
   text beside the bar, and badges carry an icon plus a label.

- [ ] **Step 1: Write the failing cell-view test**

Create `frontend/src/app/report/cell-view.spec.ts`:

```ts
import { describe, expect, it } from 'vitest';
import { barGeometry, expiryBadge } from './cell-view';

describe('expiryBadge', () => {
  it('flags an expiry before the last trade date as expired, worded relative to trade date', () => {
    const badge = expiryBadge('2010-08-15', '2010-08-20');

    expect(badge.status).toBe('expired');
    // Must not read as live status — the data is historical.
    expect(badge.label).toBe('expired as of trade date');
  });

  it('flags an expiry within seven days of the last trade date as near', () => {
    const badge = expiryBadge('2010-08-25', '2010-08-20');

    expect(badge.status).toBe('near');
    expect(badge.label).toBe('5 days from trade date');
  });

  it('uses the singular for one day', () => {
    expect(expiryBadge('2010-08-21', '2010-08-20').label).toBe('1 day from trade date');
  });

  it('treats same-day expiry as near', () => {
    const badge = expiryBadge('2010-08-20', '2010-08-20');

    expect(badge.status).toBe('near');
    expect(badge.label).toBe('expires on trade date');
  });

  it('does not badge an expiry comfortably beyond the trade date', () => {
    // The real sample case: 2010-08-20 trade, 2010-09-10 expiry = 21 days.
    const badge = expiryBadge('2010-09-10', '2010-08-20');

    expect(badge.status).toBe('normal');
    expect(badge.days).toBe(21);
  });

  it('cannot measure without a trade date, so does not badge', () => {
    const badge = expiryBadge('2010-09-10', null);

    expect(badge.status).toBe('normal');
    expect(badge.days).toBeNull();
  });
});

describe('barGeometry', () => {
  it('extends right for a positive net', () => {
    expect(barGeometry(100, 200)).toEqual({ side: 'long', percent: 50 });
  });

  it('extends left for a negative net', () => {
    expect(barGeometry(-200, 200)).toEqual({ side: 'short', percent: 100 });
  });

  it('is flat at zero', () => {
    expect(barGeometry(0, 200)).toEqual({ side: 'flat', percent: 0 });
  });

  it('does not divide by zero when every row is flat', () => {
    expect(barGeometry(0, 0)).toEqual({ side: 'flat', percent: 0 });
  });

  it('scales to the largest absolute value in view', () => {
    expect(barGeometry(50, 100).percent).toBe(50);
    expect(barGeometry(50, 500).percent).toBe(10);
  });
});
```

- [ ] **Step 2: Implement `cell-view.ts`**

Create `frontend/src/app/report/cell-view.ts`:

```ts
export type ExpiryStatus = 'expired' | 'near' | 'normal';

export interface ExpiryBadge {
  status: ExpiryStatus;
  label: string;
  /** Days from the last trade date to expiry; null when it cannot be measured. */
  days: number | null;
}

const NEAR_EXPIRY_DAYS = 7;
const MS_PER_DAY = 86_400_000;

/**
 * Days-to-expiry measured against the row's LAST TRADE DATE, deliberately not
 * against today. The sample data expires in 2010, so a wall-clock comparison
 * would badge every row "expired" — true but informationally empty. Relative to
 * the trade date the number describes the data, which is what the reader wants.
 *
 * Labels name the reference point ("as of trade date") so a historical report is
 * never mistaken for live contract status.
 */
export function expiryBadge(
  expirationDate: string,
  lastTransactionDate: string | null,
): ExpiryBadge {
  if (!lastTransactionDate) {
    return { status: 'normal', label: '', days: null };
  }

  const expiry = Date.parse(expirationDate);
  const traded = Date.parse(lastTransactionDate);
  if (Number.isNaN(expiry) || Number.isNaN(traded)) {
    return { status: 'normal', label: '', days: null };
  }

  const days = Math.round((expiry - traded) / MS_PER_DAY);

  if (days < 0) {
    return { status: 'expired', label: 'expired as of trade date', days };
  }
  if (days === 0) {
    return { status: 'near', label: 'expires on trade date', days };
  }
  if (days <= NEAR_EXPIRY_DAYS) {
    return { status: 'near', label: `${days} day${days === 1 ? '' : 's'} from trade date`, days };
  }
  return { status: 'normal', label: '', days };
}

export interface BarGeometry {
  side: 'long' | 'short' | 'flat';
  /** 0–100, relative to the largest absolute value currently in view. */
  percent: number;
}

export function barGeometry(value: number, maxAbsolute: number): BarGeometry {
  if (value === 0 || maxAbsolute === 0) {
    return { side: 'flat', percent: 0 };
  }
  return {
    side: value > 0 ? 'long' : 'short',
    percent: Math.round((Math.abs(value) / maxAbsolute) * 100),
  };
}
```

- [ ] **Step 3: Run the cell-view test**

Run: `cd frontend && npx vitest run src/app/report/cell-view.spec.ts`
Expected: PASS — 11 tests.

- [ ] **Step 4: Write the failing table test**

Create `frontend/src/app/report/report-table.spec.ts`:

```ts
import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { ReportTable } from './report-table';
import { ReportFilters } from './report-filters';
import { ColumnPreferences } from './column-preferences';
import { REPORT_COLUMNS } from './report-columns';
import { ReportEntry } from './report-entry';

function row(overrides: Partial<ReportEntry> = {}): ReportEntry {
  return {
    Client_Information: 'CL432100020001',
    Product_Information: 'SGXFUNK20100910',
    Total_Transaction_Amount: 46,
    clientType: 'CL',
    clientNumber: '4321',
    accountNumber: '0002',
    subaccountNumber: '0001',
    exchangeCode: 'SGX',
    productGroupCode: 'FU',
    symbol: 'NK',
    expirationDate: '2010-09-10',
    grossLong: 46,
    grossShort: 0,
    tradeCount: 3,
    firstTransactionDate: '2010-08-19',
    lastTransactionDate: '2010-08-20',
    lastUpdatedAt: '2026-08-12T14:31:52Z',
    feesByCurrency: { USD: -0.9 },
    ...overrides,
  };
}

function setup(rows: ReportEntry[], visibleIds = ['clientNumber', 'accountNumber', 'netQuantity']) {
  const filters = {
    rows: signal(rows),
    totalCount: signal(rows.length),
    activeFilterCount: signal(0),
    sortColumnId: signal<string | null>(null),
    sortDirection: signal<'asc' | 'desc'>('asc'),
    toggleSort: vi.fn(),
  };
  const prefs = {
    visibleColumns: signal(REPORT_COLUMNS.filter((c) => visibleIds.includes(c.id))),
  };
  TestBed.configureTestingModule({
    imports: [ReportTable],
    providers: [
      { provide: ReportFilters, useValue: filters },
      { provide: ColumnPreferences, useValue: prefs },
    ],
  });
  return { filters, prefs };
}

describe('ReportTable', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('renders one header cell per visible column, in declaration order', async () => {
    setup([row()]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    const headers = [...fixture.nativeElement.querySelectorAll('thead th')].map((th: HTMLElement) =>
      th.textContent?.trim(),
    );
    expect(headers.length).toBe(3);
    expect(headers[0]).toContain('Client');
    expect(headers[1]).toContain('Account');
    expect(headers[2]).toContain('Net');
  });

  it('renders exactly as many body cells per row as there are headers', async () => {
    // The invariant a column-definition-driven table exists to guarantee.
    setup([row(), row({ clientNumber: '1234' })], [
      'clientNumber',
      'accountNumber',
      'symbol',
      'netQuantity',
      'tradeCount',
    ]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    const headerCount = fixture.nativeElement.querySelectorAll('thead th').length;
    const bodyRows = fixture.nativeElement.querySelectorAll('tbody tr');
    expect(bodyRows.length).toBe(2);
    for (const bodyRow of bodyRows) {
      expect(bodyRow.querySelectorAll('td').length).toBe(headerCount);
    }
  });

  it('shows the signed net value as text, never colour alone', async () => {
    setup([row({ Total_Transaction_Amount: -215 })]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('tbody').textContent).toContain('-215');
  });

  it('labels a flat row', async () => {
    setup([row({ Total_Transaction_Amount: 0 })]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('flat');
  });

  it('wires a header click to toggleSort', async () => {
    const { filters } = setup([row()]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    fixture.nativeElement.querySelector('thead th button').click();

    expect(filters.toggleSort).toHaveBeenCalledWith('clientNumber');
  });

  it('shows a filtered-empty message distinct from a genuinely empty report', async () => {
    const { filters } = setup([]);
    filters.totalCount.set(5);
    filters.activeFilterCount.set(1);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('No rows match');
  });

  it('shows the expiry badge relative to trade date', async () => {
    setup([row({ expirationDate: '2010-08-22', lastTransactionDate: '2010-08-20' })], [
      'expirationDate',
    ]);
    const fixture = TestBed.createComponent(ReportTable);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('2 days from trade date');
  });
});
```

- [ ] **Step 5: Run the test to verify it fails**

Run: `cd frontend && npx vitest run src/app/report/report-table.spec.ts`
Expected: FAIL — cannot resolve `./report-table`.

- [ ] **Step 6: Implement the component**

Create `frontend/src/app/report/report-table.ts`:

```ts
import { Component, computed, inject } from '@angular/core';
import { ColumnPreferences } from './column-preferences';
import { ReportFilters } from './report-filters';
import { ColumnDef } from './report-columns';
import { ReportEntry } from './report-entry';
import { BarGeometry, ExpiryBadge, barGeometry, expiryBadge } from './cell-view';
import { formatDateTime } from './format';

@Component({
  selector: 'app-report-table',
  templateUrl: './report-table.html',
})
export class ReportTable {
  protected readonly filters = inject(ReportFilters);
  protected readonly columnPreferences = inject(ColumnPreferences);

  /** The bar scales to the largest absolute net currently in view, not overall. */
  protected readonly maxAbsoluteNet = computed(() =>
    this.filters.rows().reduce((max, row) => Math.max(max, Math.abs(row.Total_Transaction_Amount)), 0),
  );

  protected cellText(column: ColumnDef, entry: ReportEntry): string {
    const value = column.sortValue(entry);
    if (column.render === 'date') {
      return formatDateTime(typeof value === 'string' && value !== '' ? value : null);
    }
    return String(value);
  }

  protected bar(entry: ReportEntry): BarGeometry {
    return barGeometry(entry.Total_Transaction_Amount, this.maxAbsoluteNet());
  }

  protected expiry(entry: ReportEntry): ExpiryBadge {
    return expiryBadge(entry.expirationDate, entry.lastTransactionDate);
  }

  protected sortIndicator(column: ColumnDef): string {
    if (this.filters.sortColumnId() !== column.id) return '';
    return this.filters.sortDirection() === 'asc' ? '▲' : '▼';
  }

  protected rowKey(entry: ReportEntry): string {
    return entry.Client_Information + '|' + entry.Product_Information;
  }
}
```

Create `frontend/src/app/report/report-table.html`:

```html
<!--
  Wide column selections scroll inside this container so the page body never
  scrolls horizontally.
-->
<div class="overflow-x-auto rounded-lg border border-rule bg-surface-1">
  <table class="w-full border-collapse text-sm">
    <thead class="sticky top-0 bg-surface-1">
      <tr class="border-b border-rule">
        @for (column of columnPreferences.visibleColumns(); track column.id) {
          <th
            scope="col"
            class="px-3 py-2 font-semibold text-ink-secondary"
            [class.text-right]="column.align === 'right'"
            [class.text-left]="column.align === 'left'"
            [attr.aria-sort]="
              filters.sortColumnId() === column.id
                ? (filters.sortDirection() === 'asc' ? 'ascending' : 'descending')
                : 'none'
            "
          >
            <button
              type="button"
              class="inline-flex items-center gap-1 hover:text-ink-primary"
              (click)="filters.toggleSort(column.id)"
            >
              {{ column.label }}
              <span aria-hidden="true" class="text-ink-muted">{{ sortIndicator(column) }}</span>
            </button>
          </th>
        }
      </tr>
    </thead>

    <tbody>
      @for (entry of filters.rows(); track rowKey(entry)) {
        <tr class="border-b border-rule last:border-0 hover:bg-surface-page">
          @for (column of columnPreferences.visibleColumns(); track column.id) {
            <td
              class="px-3 py-2 align-middle"
              [class.text-right]="column.align === 'right'"
              [class.tabular]="column.numeric"
            >
              @switch (column.render) {
                @case ('divergingBar') {
                  <!--
                    Diverging bar off a centre baseline. The signed number is always
                    present as text, so colour is never the only channel.
                  -->
                  <div class="flex items-center justify-end gap-2">
                    <span class="tabular text-ink-primary">
                      {{ entry.Total_Transaction_Amount }}
                    </span>
                    @if (bar(entry).side === 'flat') {
                      <span class="text-xs text-ink-muted">flat</span>
                    }
                    <span class="relative hidden h-2 w-24 shrink-0 sm:block" aria-hidden="true">
                      <span
                        class="absolute inset-y-0 left-1/2 w-px bg-rule"
                      ></span>
                      @if (bar(entry).side === 'long') {
                        <span
                          class="absolute inset-y-0 left-1/2 rounded-r bg-net-long"
                          [style.width.%]="bar(entry).percent / 2"
                        ></span>
                      }
                      @if (bar(entry).side === 'short') {
                        <span
                          class="absolute inset-y-0 right-1/2 rounded-l bg-net-short"
                          [style.width.%]="bar(entry).percent / 2"
                        ></span>
                      }
                    </span>
                  </div>
                }
                @case ('expiry') {
                  <div class="flex items-center gap-2">
                    <span>{{ entry.expirationDate }}</span>
                    @if (expiry(entry).status === 'expired') {
                      <span
                        class="rounded bg-status-critical/15 px-1.5 py-0.5 text-xs text-status-critical"
                      >
                        ⚠ {{ expiry(entry).label }}
                      </span>
                    }
                    @if (expiry(entry).status === 'near') {
                      <span
                        class="rounded bg-status-warning/20 px-1.5 py-0.5 text-xs text-ink-primary"
                      >
                        ◷ {{ expiry(entry).label }}
                      </span>
                    }
                  </div>
                }
                @default {
                  {{ cellText(column, entry) }}
                }
              }
            </td>
          }
        </tr>
      } @empty {
        <tr>
          <td
            [attr.colspan]="columnPreferences.visibleColumns().length"
            class="px-3 py-8 text-center text-ink-secondary"
          >
            @if (filters.activeFilterCount() > 0) {
              No rows match the current filters ({{ filters.totalCount() }} total).
            } @else {
              No transactions recorded yet.
            }
          </td>
        </tr>
      }
    </tbody>
  </table>
</div>
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `cd frontend && npx vitest run src/app/report/report-table.spec.ts`
Expected: PASS — 7 tests.

- [ ] **Step 8: Write the failing changed-row test**

A live table should show what just moved. Append to `cell-view.spec.ts`:

```ts
import { changedKeys } from './cell-view';

describe('changedKeys', () => {
  const key = (r: { c: string; p: string }) => r.c + '|' + r.p;

  it('reports nothing on the first snapshot', () => {
    // Everything is "new" initially; flashing every row on load would be noise.
    const rows = [{ k: 'a', updated: 't1' }];

    expect(changedKeys(null, rows, (r) => r.k, (r) => r.updated).size).toBe(0);
  });

  it('reports a row whose timestamp advanced', () => {
    const before = new Map([['a', 't1'], ['b', 't1']]);
    const rows = [{ k: 'a', updated: 't2' }, { k: 'b', updated: 't1' }];

    const changed = changedKeys(before, rows, (r) => r.k, (r) => r.updated);

    expect([...changed]).toEqual(['a']);
  });

  it('reports a newly appeared row', () => {
    const before = new Map([['a', 't1']]);
    const rows = [{ k: 'a', updated: 't1' }, { k: 'b', updated: 't1' }];

    expect([...changedKeys(before, rows, (r) => r.k, (r) => r.updated)]).toEqual(['b']);
  });

  it('reports nothing when a poll returns identical data', () => {
    const before = new Map([['a', 't1']]);
    const rows = [{ k: 'a', updated: 't1' }];

    expect(changedKeys(before, rows, (r) => r.k, (r) => r.updated).size).toBe(0);
  });
});
```

- [ ] **Step 9: Implement `changedKeys` and wire the flash**

Append to `frontend/src/app/report/cell-view.ts`:

```ts
/**
 * Keys whose timestamp advanced since the previous snapshot, plus keys that are
 * new. Returns empty for a null previous snapshot: on first load every row would
 * qualify, and flashing the whole table conveys nothing.
 */
export function changedKeys<T>(
  previous: ReadonlyMap<string, string | null> | null,
  rows: readonly T[],
  keyOf: (row: T) => string,
  updatedAtOf: (row: T) => string | null,
): Set<string> {
  if (previous === null) {
    return new Set();
  }
  const changed = new Set<string>();
  for (const row of rows) {
    const key = keyOf(row);
    if (!previous.has(key) || previous.get(key) !== updatedAtOf(row)) {
      changed.add(key);
    }
  }
  return changed;
}
```

In `report-table.ts`, add the snapshot tracking. Import `effect`, `signal` and
`changedKeys`, then add to the class:

```ts
  private previousSnapshot: Map<string, string | null> | null = null;
  private readonly _changed = signal<ReadonlySet<string>>(new Set());

  protected readonly changed = this._changed.asReadonly();

  constructor() {
    // Recompute whenever a poll replaces the rows.
    effect(() => {
      const rows = this.filters.rows();
      this._changed.set(
        changedKeys(this.previousSnapshot, rows, (row) => this.rowKey(row), (row) => row.lastUpdatedAt),
      );
      this.previousSnapshot = new Map(rows.map((row) => [this.rowKey(row), row.lastUpdatedAt]));
    });
  }
```

Add the component-scoped animation to the `@Component` decorator (the global
`prefers-reduced-motion` rule from Task 11 already neutralises it for users who
ask for that):

```ts
  styles: [
    `
      @keyframes row-flash {
        from {
          background-color: color-mix(in oklab, var(--net-long) 18%, transparent);
        }
        to {
          background-color: transparent;
        }
      }
      .row-changed {
        animation: row-flash 1.2s ease-out;
      }
    `,
  ],
```

In `report-table.html`, add the class binding to the body row:

```html
        <tr
          class="border-b border-rule last:border-0 hover:bg-surface-page"
          [class.row-changed]="changed().has(rowKey(entry))"
        >
```

- [ ] **Step 10: Run the tests to verify they pass**

Run: `cd frontend && npx vitest run src/app/report/cell-view.spec.ts src/app/report/report-table.spec.ts`
Expected: PASS — 15 cell-view tests plus 7 table tests.

- [ ] **Step 11: Commit**

```bash
git add frontend/src/app/report/cell-view.ts frontend/src/app/report/cell-view.spec.ts \
        frontend/src/app/report/report-table.ts frontend/src/app/report/report-table.html \
        frontend/src/app/report/report-table.spec.ts
git commit -m "feat(frontend): add the report table with diverging net bar, expiry badges and change flash"
```

---

## Task 17: Filter bar, column picker, refresh control

**Files:**
- Create: `frontend/src/app/report/filter-bar.ts`
- Create: `frontend/src/app/report/column-picker.ts`
- Create: `frontend/src/app/report/refresh-control.ts`
- Test: `frontend/src/app/report/filter-bar.spec.ts`
- Test: `frontend/src/app/report/refresh-control.spec.ts`

**Interfaces:**
- Consumes: `ReportFilters` (Task 14), `ColumnPreferences` + `COLUMN_GROUPS`
  (Task 13), `ReportService` (Task 12), `formatRelative` (Task 15).
- Produces: `FilterBar` (`app-filter-bar`), `ColumnPicker` (`app-column-picker`),
  `RefreshControl` (`app-refresh-control`). Task 19 composes all three.

- [ ] **Step 1: Write the failing tests**

Create `frontend/src/app/report/filter-bar.spec.ts`:

```ts
import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FilterBar } from './filter-bar';
import { ReportFilters } from './report-filters';

function setup() {
  const filters = {
    client: signal(''),
    account: signal(''),
    product: signal(''),
    search: signal(''),
    clientOptions: signal(['CL123400020001', 'CL432100020001']),
    accountOptions: signal(['0002', '0003']),
    productOptions: signal(['CMEFUNK.20100910', 'SGXFUNK20100910']),
    rows: signal([]),
    totalCount: signal(5),
    activeFilterCount: signal(0),
    setClient: vi.fn(),
    setAccount: vi.fn(),
    setProduct: vi.fn(),
    setSearch: vi.fn(),
    clearAll: vi.fn(),
  };
  TestBed.configureTestingModule({
    imports: [FilterBar],
    providers: [{ provide: ReportFilters, useValue: filters }],
  });
  return filters;
}

describe('FilterBar', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('populates each dimension select from the data, plus an All option', async () => {
    setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    const clientSelect: HTMLSelectElement = fixture.nativeElement.querySelector(
      'select[data-testid="filter-client"]',
    );
    // 2 values + "All"
    expect(clientSelect.options.length).toBe(3);
    expect(clientSelect.options[0].textContent).toContain('All');
  });

  it('reports the visible-of-total count', async () => {
    const filters = setup();
    filters.rows.set([{} as never, {} as never]);
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('2 of 5');
  });

  it('shows a clear-all control only while a filter is active', async () => {
    const filters = setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();
    expect(fixture.nativeElement.querySelector('[data-testid="clear-filters"]')).toBeNull();

    filters.activeFilterCount.set(1);
    fixture.detectChanges();
    await fixture.whenStable();
    expect(fixture.nativeElement.querySelector('[data-testid="clear-filters"]')).not.toBeNull();
  });

  it('forwards a search entry', async () => {
    const filters = setup();
    const fixture = TestBed.createComponent(FilterBar);
    fixture.detectChanges();
    await fixture.whenStable();

    const input: HTMLInputElement = fixture.nativeElement.querySelector(
      'input[data-testid="filter-search"]',
    );
    input.value = 'NK';
    input.dispatchEvent(new Event('input'));

    expect(filters.setSearch).toHaveBeenCalledWith('NK');
  });
});
```

Create `frontend/src/app/report/refresh-control.spec.ts`:

```ts
import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { RefreshControl } from './refresh-control';
import { ReportService } from './report.service';

function setup(autoRefresh: boolean, stale = false) {
  const service = {
    autoRefresh: signal(autoRefresh),
    stale: signal(stale),
    lastLoadedAt: signal<Date | null>(new Date('2026-08-12T14:31:52Z')),
    errorMessage: signal<string | null>(stale ? 'network down' : null),
    setAutoRefresh: vi.fn(),
    refresh: vi.fn(),
  };
  TestBed.configureTestingModule({
    imports: [RefreshControl],
    providers: [{ provide: ReportService, useValue: service }],
  });
  return service;
}

describe('RefreshControl', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('disables the manual button while auto-refresh is on', async () => {
    setup(true);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    const button: HTMLButtonElement = fixture.nativeElement.querySelector(
      'button[data-testid="refresh"]',
    );
    expect(button.disabled).toBe(true);
  });

  it('enables the manual button when auto-refresh is off and wires it', async () => {
    const service = setup(false);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    const button: HTMLButtonElement = fixture.nativeElement.querySelector(
      'button[data-testid="refresh"]',
    );
    expect(button.disabled).toBe(false);
    button.click();
    expect(service.refresh).toHaveBeenCalled();
  });

  it('toggling the switch forwards the new state', async () => {
    const service = setup(true);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    const toggle: HTMLInputElement = fixture.nativeElement.querySelector(
      'input[data-testid="auto-refresh"]',
    );
    toggle.click();

    expect(service.setAutoRefresh).toHaveBeenCalledWith(false);
  });

  it('shows a stale badge when a refresh has failed', async () => {
    setup(true, true);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="stale-badge"]')).not.toBeNull();
    expect(fixture.nativeElement.textContent).toContain('network down');
  });

  it('shows no stale badge when healthy', async () => {
    setup(true, false);
    const fixture = TestBed.createComponent(RefreshControl);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="stale-badge"]')).toBeNull();
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd frontend && npx vitest run src/app/report/filter-bar.spec.ts src/app/report/refresh-control.spec.ts`
Expected: FAIL — cannot resolve the components.

- [ ] **Step 3: Implement `FilterBar`**

Create `frontend/src/app/report/filter-bar.ts`:

```ts
import { Component, inject } from '@angular/core';
import { ReportFilters } from './report-filters';

@Component({
  selector: 'app-filter-bar',
  template: `
    <div class="flex flex-wrap items-end gap-3">
      <label class="flex flex-col gap-1 text-xs text-ink-muted">
        Client
        <select
          data-testid="filter-client"
          class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
          [value]="filters.client()"
          (change)="filters.setClient($any($event.target).value)"
        >
          <option value="">All ({{ filters.clientOptions().length }})</option>
          @for (option of filters.clientOptions(); track option) {
            <option [value]="option">{{ option }}</option>
          }
        </select>
      </label>

      <label class="flex flex-col gap-1 text-xs text-ink-muted">
        Account
        <select
          data-testid="filter-account"
          class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
          [value]="filters.account()"
          (change)="filters.setAccount($any($event.target).value)"
        >
          <option value="">All ({{ filters.accountOptions().length }})</option>
          @for (option of filters.accountOptions(); track option) {
            <option [value]="option">{{ option }}</option>
          }
        </select>
      </label>

      <label class="flex flex-col gap-1 text-xs text-ink-muted">
        Product
        <select
          data-testid="filter-product"
          class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
          [value]="filters.product()"
          (change)="filters.setProduct($any($event.target).value)"
        >
          <option value="">All ({{ filters.productOptions().length }})</option>
          @for (option of filters.productOptions(); track option) {
            <option [value]="option">{{ option }}</option>
          }
        </select>
      </label>

      <label class="flex flex-col gap-1 text-xs text-ink-muted">
        Search
        <input
          type="search"
          data-testid="filter-search"
          placeholder="any field"
          class="rounded border border-rule bg-surface-1 px-2 py-1 text-sm text-ink-primary"
          [value]="filters.search()"
          (input)="filters.setSearch($any($event.target).value)"
        />
      </label>

      <p class="ml-auto text-sm text-ink-secondary" aria-live="polite">
        {{ filters.rows().length }} of {{ filters.totalCount() }} rows
        @if (filters.activeFilterCount() > 0) {
          <button
            type="button"
            data-testid="clear-filters"
            class="ml-2 underline hover:text-ink-primary"
            (click)="filters.clearAll()"
          >
            Clear filters
          </button>
        }
      </p>
    </div>
  `,
})
export class FilterBar {
  protected readonly filters = inject(ReportFilters);
}
```

- [ ] **Step 4: Implement `ColumnPicker`**

Create `frontend/src/app/report/column-picker.ts`:

```ts
import { Component, inject, signal } from '@angular/core';
import { ColumnPreferences } from './column-preferences';
import { COLUMN_GROUPS, ColumnGroup, REPORT_COLUMNS } from './report-columns';

@Component({
  selector: 'app-column-picker',
  template: `
    <div class="relative">
      <button
        type="button"
        data-testid="column-picker-toggle"
        class="rounded border border-rule px-2 py-1 text-sm text-ink-secondary hover:text-ink-primary"
        [attr.aria-expanded]="open()"
        (click)="open.set(!open())"
      >
        Columns ({{ columnPreferences.visibleIds().length }}) ▾
      </button>

      @if (open()) {
        <div
          class="absolute right-0 z-10 mt-1 max-h-80 w-64 overflow-y-auto rounded-lg border border-rule bg-surface-1 p-3 shadow-lg"
        >
          @for (group of groups; track group) {
            <p class="mt-2 text-xs font-semibold uppercase tracking-wide text-ink-muted first:mt-0">
              {{ group }}
            </p>
            @for (column of columnsIn(group); track column.id) {
              <label class="flex items-center gap-2 py-1 text-sm text-ink-primary">
                <input
                  type="checkbox"
                  [attr.data-testid]="'column-' + column.id"
                  [checked]="columnPreferences.isVisible(column.id)"
                  (change)="columnPreferences.toggle(column.id)"
                />
                {{ column.label }}
              </label>
            }
          }
          <button
            type="button"
            data-testid="reset-columns"
            class="mt-3 w-full rounded border border-rule py-1 text-sm text-ink-secondary hover:text-ink-primary"
            (click)="columnPreferences.reset()"
          >
            Reset to defaults
          </button>
        </div>
      }
    </div>
  `,
})
export class ColumnPicker {
  protected readonly columnPreferences = inject(ColumnPreferences);
  protected readonly open = signal(false);
  protected readonly groups = COLUMN_GROUPS;

  protected columnsIn(group: ColumnGroup) {
    return REPORT_COLUMNS.filter((column) => column.group === group);
  }
}
```

- [ ] **Step 5: Implement `RefreshControl`**

Create `frontend/src/app/report/refresh-control.ts`:

```ts
import { Component, inject } from '@angular/core';
import { ReportService } from './report.service';
import { formatRelative } from './format';

@Component({
  selector: 'app-refresh-control',
  template: `
    <div class="flex flex-wrap items-center gap-3 text-sm">
      <label class="flex items-center gap-2 text-ink-secondary">
        <input
          type="checkbox"
          data-testid="auto-refresh"
          [checked]="reportService.autoRefresh()"
          (change)="reportService.setAutoRefresh($any($event.target).checked)"
        />
        Auto-refresh
      </label>

      <!-- Manual refresh is only meaningful when polling is off. -->
      <button
        type="button"
        data-testid="refresh"
        class="rounded border border-rule px-2 py-1 text-ink-secondary hover:text-ink-primary disabled:opacity-40"
        [disabled]="reportService.autoRefresh()"
        (click)="reportService.refresh()"
      >
        Refresh
      </button>

      <span class="text-ink-muted" data-testid="last-updated">
        updated {{ relative(reportService.lastLoadedAt()) }}
      </span>

      @if (reportService.stale()) {
        <span
          data-testid="stale-badge"
          class="rounded bg-status-warning/20 px-1.5 py-0.5 text-xs text-ink-primary"
        >
          ⚠ stale — {{ reportService.errorMessage() }}
        </span>
      }
    </div>
  `,
})
export class RefreshControl {
  protected readonly reportService = inject(ReportService);

  protected relative(date: Date | null): string {
    return formatRelative(date === null ? null : date.toISOString());
  }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cd frontend && npx vitest run src/app/report/filter-bar.spec.ts src/app/report/refresh-control.spec.ts`
Expected: PASS — 9 tests.

- [ ] **Step 7: Commit**

```bash
git add frontend/src/app/report/filter-bar.ts frontend/src/app/report/filter-bar.spec.ts \
        frontend/src/app/report/column-picker.ts \
        frontend/src/app/report/refresh-control.ts \
        frontend/src/app/report/refresh-control.spec.ts
git commit -m "feat(frontend): add filter bar, column picker and refresh control"
```

---

## Task 18: KPI row

**Files:**
- Create: `frontend/src/app/report/kpi-row.ts`
- Test: `frontend/src/app/report/kpi-row.spec.ts`

**Interfaces:**
- Consumes: `ReportFilters` (Task 14), `IngestionStatusService` (Task 15).
- Produces: `KpiRow` standalone component, selector `app-kpi-row`.

**There is deliberately no "total net quantity" tile.** Summing net quantity
across different contracts adds quantities of different instruments, which is not
a number. Every tile here is genuinely additive.

**Fee totals are per currency and are negative.** `D` = debit = negative, and
every money field in the sample carries `D`, so `USD -0.90` is correct output —
not a sign bug to "fix".

`sum(tradeCount)` versus the file's `published` count is a real reconciliation: a
mismatch means records were lost between ingestion and aggregation, so the tile
carries a warning when they disagree.

- [ ] **Step 1: Write the failing test**

Create `frontend/src/app/report/kpi-row.spec.ts`:

```ts
import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it } from 'vitest';
import { KpiRow } from './kpi-row';
import { ReportFilters } from './report-filters';
import { IngestionStatusService } from './ingestion-status.service';
import { IngestionStatus, ReportEntry } from './report-entry';

function row(overrides: Partial<ReportEntry> = {}): ReportEntry {
  return {
    Client_Information: 'CL432100020001',
    Product_Information: 'SGXFUNK20100910',
    Total_Transaction_Amount: 46,
    clientType: 'CL',
    clientNumber: '4321',
    accountNumber: '0002',
    subaccountNumber: '0001',
    exchangeCode: 'SGX',
    productGroupCode: 'FU',
    symbol: 'NK',
    expirationDate: '2010-09-10',
    grossLong: 46,
    grossShort: 0,
    tradeCount: 3,
    firstTransactionDate: '2010-08-19',
    lastTransactionDate: '2010-08-20',
    lastUpdatedAt: '2026-08-12T14:31:52Z',
    feesByCurrency: { USD: -0.9 },
    ...overrides,
  };
}

function setup(rows: ReportEntry[], status: IngestionStatus | null) {
  TestBed.configureTestingModule({
    imports: [KpiRow],
    providers: [
      { provide: ReportFilters, useValue: { rows: signal(rows) } },
      { provide: IngestionStatusService, useValue: { status: signal(status) } },
    ],
  });
}

describe('KpiRow', () => {
  beforeEach(() => TestBed.resetTestingModule());

  it('sums trade counts and counts pairs and distinct clients', async () => {
    setup(
      [
        row({ tradeCount: 3 }),
        row({ tradeCount: 4, Client_Information: 'CL123400030001' }),
        row({ tradeCount: 5, Client_Information: 'CL123400030001', symbol: 'N1' }),
      ],
      null,
    );
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    const text = fixture.nativeElement.textContent;
    expect(fixture.nativeElement.querySelector('[data-testid="kpi-transactions"]').textContent)
      .toContain('12');
    expect(fixture.nativeElement.querySelector('[data-testid="kpi-pairs"]').textContent)
      .toContain('3');
    expect(fixture.nativeElement.querySelector('[data-testid="kpi-clients"]').textContent)
      .toContain('2');
    expect(text).toBeTruthy();
  });

  it('renders one figure per currency and never blends them', async () => {
    setup(
      [
        row({ feesByCurrency: { USD: -0.9, JPY: -120 } }),
        row({ feesByCurrency: { USD: -0.15 } }),
      ],
      null,
    );
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    const fees = fixture.nativeElement.querySelector('[data-testid="kpi-fees"]').textContent;
    expect(fees).toContain('USD');
    expect(fees).toContain('-1.05');
    expect(fees).toContain('JPY');
    expect(fees).toContain('-120');
  });

  it('shows an em dash for fees when there are none', async () => {
    setup([row({ feesByCurrency: {} })], null);
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="kpi-fees"]').textContent)
      .toContain('—');
  });

  it('warns when aggregated trades disagree with records published', async () => {
    setup([row({ tradeCount: 700 })], {
      configuredPath: 'sample-data/Input.txt',
      fileExists: true,
      fileSizeBytes: 127624,
      fileLastModified: null,
      lastIngestAt: '2026-08-12T14:31:52Z',
      fingerprint: 'fp',
      totalLines: 717,
      published: 717,
      skipped: 0,
      errorCount: 0,
    });
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="reconcile-warning"]')).not.toBeNull();
  });

  it('does not warn when they agree', async () => {
    setup([row({ tradeCount: 717 })], {
      configuredPath: 'sample-data/Input.txt',
      fileExists: true,
      fileSizeBytes: 127624,
      fileLastModified: null,
      lastIngestAt: '2026-08-12T14:31:52Z',
      fingerprint: 'fp',
      totalLines: 717,
      published: 717,
      skipped: 0,
      errorCount: 0,
    });
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="reconcile-warning"]')).toBeNull();
  });

  it('does not warn when the status endpoint is unavailable', async () => {
    setup([row({ tradeCount: 3 })], null);
    const fixture = TestBed.createComponent(KpiRow);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="reconcile-warning"]')).toBeNull();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd frontend && npx vitest run src/app/report/kpi-row.spec.ts`
Expected: FAIL — cannot resolve `./kpi-row`.

- [ ] **Step 3: Implement**

Create `frontend/src/app/report/kpi-row.ts`:

```ts
import { Component, computed, inject } from '@angular/core';
import { ReportFilters } from './report-filters';
import { IngestionStatusService } from './ingestion-status.service';

@Component({
  selector: 'app-kpi-row',
  template: `
    <dl class="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-transactions">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Transactions</dt>
        <dd class="mt-1 text-2xl text-ink-primary">{{ transactions() }}</dd>
        @if (reconciliationMismatch()) {
          <p
            data-testid="reconcile-warning"
            class="mt-1 text-xs text-status-critical"
          >
            ⚠ {{ published() }} published — {{ transactions() }} aggregated
          </p>
        }
      </div>

      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-pairs">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Client/product pairs</dt>
        <dd class="mt-1 text-2xl text-ink-primary">{{ filters.rows().length }}</dd>
      </div>

      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-clients">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Distinct clients</dt>
        <dd class="mt-1 text-2xl text-ink-primary">{{ distinctClients() }}</dd>
      </div>

      <div class="rounded-lg border border-rule bg-surface-1 p-3" data-testid="kpi-fees">
        <dt class="text-xs uppercase tracking-wide text-ink-muted">Fees</dt>
        <dd class="mt-1 text-sm text-ink-primary">
          @if (feeEntries().length === 0) {
            <span class="text-2xl">—</span>
          } @else {
            <!-- One figure per currency: two currencies are never added together. -->
            @for (fee of feeEntries(); track fee.currency) {
              <span class="tabular mr-3 whitespace-nowrap">
                {{ fee.currency }} {{ fee.amount }}
              </span>
            }
          }
        </dd>
      </div>
    </dl>
  `,
})
export class KpiRow {
  protected readonly filters = inject(ReportFilters);
  private readonly statusService = inject(IngestionStatusService);

  protected readonly transactions = computed(() =>
    this.filters.rows().reduce((total, row) => total + row.tradeCount, 0),
  );

  protected readonly distinctClients = computed(
    () => new Set(this.filters.rows().map((row) => row.Client_Information)).size,
  );

  protected readonly published = computed(() => this.statusService.status()?.published ?? null);

  /**
   * A mismatch means records were lost between ingestion and aggregation. Only
   * meaningful with no filters applied and a known published count, so it is
   * suppressed otherwise rather than crying wolf.
   */
  protected readonly reconciliationMismatch = computed(() => {
    const published = this.published();
    if (published === null) return false;
    return published !== this.transactions();
  });

  /** Totals per currency, in a stable order. Negative is expected: D = debit. */
  protected readonly feeEntries = computed(() => {
    const totals = new Map<string, number>();
    for (const row of this.filters.rows()) {
      for (const [currency, amount] of Object.entries(row.feesByCurrency)) {
        totals.set(currency, (totals.get(currency) ?? 0) + amount);
      }
    }
    return [...totals.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([currency, amount]) => ({
        currency,
        // toFixed(2) then strip a trailing ".00" so JPY reads -120, USD reads -1.05.
        amount: amount.toFixed(2).replace(/\.00$/, ''),
      }));
  });
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd frontend && npx vitest run src/app/report/kpi-row.spec.ts`
Expected: PASS — 6 tests.

- [ ] **Step 5: Commit**

```bash
git add frontend/src/app/report/kpi-row.ts frontend/src/app/report/kpi-row.spec.ts
git commit -m "feat(frontend): add the KPI row with per-currency fees and a reconciliation check"
```

---

## Task 19: Compose the shell and verify end to end

**Files:**
- Modify: `frontend/src/app/report/report.ts`
- Modify: `frontend/src/app/report/report.html`
- Delete: `frontend/src/app/report/report.css`
- Modify: `frontend/src/app/report/report.spec.ts`
- Modify: `frontend/src/app/report/report.integration.spec.ts`
- Modify: `frontend/src/index.html`
- Modify: `README.md`, `frontend/README.md`

**Interfaces:**
- Consumes: every component from Tasks 11, 15, 16, 17, 18.
- Produces: the assembled page. Nothing downstream.

`report.css` is deleted because every rule in it is now a Tailwind utility;
leaving it would mean two sources of truth for the same styling.

**Layout order:** header (title, theme toggle) → source-file panel → KPI row →
filter bar with column picker and refresh control → table → CSV download.

- [ ] **Step 1: Rewrite the shell component**

Replace `frontend/src/app/report/report.ts`:

```ts
import { Component, OnInit, inject } from '@angular/core';
import { ReportService } from './report.service';
import { IngestionStatusService } from './ingestion-status.service';
import { SourceFilePanel } from './source-file-panel';
import { KpiRow } from './kpi-row';
import { FilterBar } from './filter-bar';
import { ColumnPicker } from './column-picker';
import { RefreshControl } from './refresh-control';
import { ReportTable } from './report-table';
import { ThemeToggle } from '../shared/theme-toggle';

@Component({
  selector: 'app-report',
  imports: [
    SourceFilePanel,
    KpiRow,
    FilterBar,
    ColumnPicker,
    RefreshControl,
    ReportTable,
    ThemeToggle,
  ],
  templateUrl: './report.html',
})
export class Report implements OnInit {
  protected readonly reportService = inject(ReportService);
  private readonly statusService = inject(IngestionStatusService);

  ngOnInit(): void {
    this.reportService.load();
    this.reportService.startPolling();
    // Independent of the report: if provenance fails to load the report still works.
    this.statusService.load();
  }

  protected retry(): void {
    this.reportService.load();
  }
}
```

- [ ] **Step 2: Rewrite the shell template**

Replace `frontend/src/app/report/report.html`:

```html
<div class="mx-auto max-w-7xl p-4 sm:p-6">
  <header class="mb-5 flex flex-wrap items-center justify-between gap-3">
    <div>
      <h1 class="text-xl font-semibold text-ink-primary">Daily Summary Report</h1>
      <p class="text-sm text-ink-secondary">
        Net transaction quantity per client and product
      </p>
    </div>
    <app-theme-toggle />
  </header>

  @if (reportService.status() === 'loading') {
    <p class="rounded-lg border border-rule bg-surface-1 p-4 text-sm text-ink-secondary">
      Report is still being generated — Kafka Streams is starting up. This can take a
      few moments.
      @if (reportService.retryCount() > 10) {
        <span data-testid="stuck-notice" class="mt-1 block text-status-warning">
          Still waiting after 30s — processing-service may not be healthy.
        </span>
      }
    </p>
  }

  @if (reportService.status() === 'error') {
    <div class="rounded-lg border border-status-critical/40 bg-status-critical/10 p-4">
      <p class="text-sm text-ink-primary">{{ reportService.errorMessage() }}</p>
      <button
        type="button"
        data-testid="retry"
        class="mt-2 rounded border border-rule px-2 py-1 text-sm text-ink-secondary hover:text-ink-primary"
        (click)="retry()"
      >
        Retry
      </button>
    </div>
  }

  @if (reportService.status() === 'ready') {
    <div class="flex flex-col gap-5">
      <app-source-file-panel />
      <app-kpi-row />

      <div class="flex flex-wrap items-end justify-between gap-3">
        <app-filter-bar class="grow" />
        <div class="flex items-center gap-3">
          <app-refresh-control />
          <app-column-picker />
        </div>
      </div>

      <app-report-table />

      <div>
        <a
          data-testid="csv-download"
          href="/api/v1/report/csv"
          download
          class="inline-block rounded border border-rule px-3 py-1.5 text-sm text-ink-secondary hover:text-ink-primary"
        >
          Download CSV
        </a>
      </div>
    </div>
  }
</div>
```

- [ ] **Step 3: Delete the obsolete stylesheet**

```bash
git rm frontend/src/app/report/report.css
```

The `styleUrl` reference was already dropped in Step 1's `@Component`.

- [ ] **Step 4: Rewrite `report.spec.ts` for the shell's own responsibilities**

The old tests asserted on the table and refresh button, which now live in child
components with their own tests. The shell's job is narrower: call the loaders on
init, and switch between loading / error / ready. Replace the file:

```ts
import { signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { Report } from './report';
import { ReportService } from './report.service';
import { ReportFilters } from './report-filters';
import { ColumnPreferences } from './column-preferences';
import { IngestionStatusService } from './ingestion-status.service';
import { REPORT_COLUMNS } from './report-columns';

function setup(overrides: {
  status: 'loading' | 'ready' | 'error';
  errorMessage?: string | null;
  retryCount?: number;
}) {
  const reportService = {
    status: signal(overrides.status),
    entries: signal([]),
    errorMessage: signal(overrides.errorMessage ?? null),
    retryCount: signal(overrides.retryCount ?? 0),
    stale: signal(false),
    lastLoadedAt: signal<Date | null>(null),
    autoRefresh: signal(true),
    load: vi.fn(),
    refresh: vi.fn(),
    startPolling: vi.fn(),
    setAutoRefresh: vi.fn(),
  };
  const statusService = { status: signal(null), available: signal(false), load: vi.fn() };
  const filters = {
    client: signal(''),
    account: signal(''),
    product: signal(''),
    search: signal(''),
    clientOptions: signal<string[]>([]),
    accountOptions: signal<string[]>([]),
    productOptions: signal<string[]>([]),
    rows: signal([]),
    totalCount: signal(0),
    activeFilterCount: signal(0),
    sortColumnId: signal<string | null>(null),
    sortDirection: signal<'asc' | 'desc'>('asc'),
    setClient: vi.fn(),
    setAccount: vi.fn(),
    setProduct: vi.fn(),
    setSearch: vi.fn(),
    clearAll: vi.fn(),
    toggleSort: vi.fn(),
  };
  const prefs = {
    visibleIds: signal(['clientNumber']),
    visibleColumns: signal(REPORT_COLUMNS.filter((c) => c.id === 'clientNumber')),
    isVisible: () => true,
    toggle: vi.fn(),
    reset: vi.fn(),
  };

  TestBed.configureTestingModule({
    imports: [Report],
    providers: [
      { provide: ReportService, useValue: reportService },
      { provide: IngestionStatusService, useValue: statusService },
      { provide: ReportFilters, useValue: filters },
      { provide: ColumnPreferences, useValue: prefs },
    ],
  });
  return { reportService, statusService };
}

describe('Report shell', () => {
  beforeEach(() => {
    localStorage.clear();
    TestBed.resetTestingModule();
  });

  it('loads the report, starts polling and loads provenance on init', () => {
    const { reportService, statusService } = setup({ status: 'loading' });

    TestBed.createComponent(Report).detectChanges();

    expect(reportService.load).toHaveBeenCalledTimes(1);
    expect(reportService.startPolling).toHaveBeenCalledTimes(1);
    expect(statusService.load).toHaveBeenCalledTimes(1);
  });

  it('shows the loading banner and no table while loading', async () => {
    setup({ status: 'loading' });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('still being generated');
    expect(fixture.nativeElement.querySelector('table')).toBeNull();
  });

  it('hides the stuck notice at or below the retry threshold', async () => {
    setup({ status: 'loading', retryCount: 10 });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="stuck-notice"]')).toBeNull();
  });

  it('shows the stuck notice past the retry threshold', async () => {
    setup({ status: 'loading', retryCount: 11 });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('[data-testid="stuck-notice"]')).not.toBeNull();
    expect(fixture.nativeElement.textContent).toContain('Still waiting after 30s');
  });

  it('shows the error banner and wires Retry to load()', async () => {
    const { reportService } = setup({ status: 'error', errorMessage: 'network down' });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.textContent).toContain('network down');
    reportService.load.mockClear();
    fixture.nativeElement.querySelector('button[data-testid="retry"]').click();
    expect(reportService.load).toHaveBeenCalledTimes(1);
  });

  it('renders the table region and CSV link when ready', async () => {
    setup({ status: 'ready' });
    const fixture = TestBed.createComponent(Report);
    fixture.detectChanges();
    await fixture.whenStable();

    expect(fixture.nativeElement.querySelector('table')).not.toBeNull();
    const csvLink: HTMLAnchorElement = fixture.nativeElement.querySelector(
      'a[data-testid="csv-download"]',
    );
    expect(csvLink.getAttribute('href')).toBe('/api/v1/report/csv');
  });
});
```

- [ ] **Step 5: Update `report.integration.spec.ts`**

This test drives the real `ReportService` against `HttpTestingController`. Two
changes are needed:

1. Any inline `ReportEntry` literal must gain the new required fields — reuse the
   `row()` factory pattern from Task 12's spec.
2. It now issues a **second** request to `/api/v1/ingest/status` on init. Either
   flush it or ignore it:

```ts
    // The shell also loads provenance on init; answer it so httpMock.verify() passes.
    httpMock.expectOne('/api/v1/ingest/status').flush({
      configuredPath: 'sample-data/Input.txt',
      fileExists: true,
      fileSizeBytes: 127624,
      fileLastModified: '2026-08-12T09:14:00Z',
      lastIngestAt: '2026-08-12T14:31:52Z',
      fingerprint: 'fp-1',
      totalLines: 717,
      published: 717,
      skipped: 0,
      errorCount: 0,
    });
```

Also add `localStorage.clear()` to its `beforeEach`, or a persisted
auto-refresh/column preference from another test can change what it renders.

- [ ] **Step 6: Set the page background**

In `frontend/src/index.html`, add the token-driven background class to `<body>` so
the page plane matches the theme rather than staying white in dark mode:

```html
<body class="bg-surface-page text-ink-primary">
```

- [ ] **Step 7: Run the whole frontend suite**

Run: `cd frontend && npm test`
Expected: PASS across every spec.

- [ ] **Step 8: Build**

Run: `cd frontend && npm run build`
Expected: succeeds within the existing 500 kB initial budget. Tailwind's output is
small because v4 only emits used utilities; if the budget trips, that is a real
signal something imported the whole framework rather than a reason to raise it.

- [ ] **Step 9: Full-stack manual verification, starting from teardown**

**Start from a torn-down state.** The migration strategy is "teardown rather than
rename", so beginning here is what actually *exercises* it — verifying against a
broker that was never wiped would silently skip the one thing that decision rests
on.

```bash
docker compose down -v --remove-orphans
docker compose up -d --build
curl -X POST http://localhost:8081/api/v1/ingest
```

Then open `http://localhost:8080` and confirm:

- [ ] Table shows 5 rows with **client number** and **account number** as columns.
- [ ] Column picker lists 17 columns in 5 groups; toggling one updates the table
      immediately; the choice survives a page reload.
- [ ] Client, Account and Product filters each populate from the data; combining
      two narrows correctly; "N of 5 rows" updates; Clear filters appears only
      while a filter is active.
- [ ] Filtering to nothing shows "No rows match the current filters", **not**
      "No transactions recorded yet".
- [ ] Sorting by Net orders numerically (`-215` before `46`, not lexicographically).
- [ ] **Sticky header actually sticks.** Select enough columns and rows to overflow,
      then scroll: the header must remain visible. This cannot be unit-tested —
      jsdom performs no layout — and the first implementation looked correct while
      being inert, because `overflow-x: auto` makes the container a scroll container
      on both axes, so an unbounded-height wrapper leaves `position: sticky`
      anchored to something that never scrolls.
- [ ] Net cells show a blue bar right for positive, red left for negative, and the
      signed number as text in every case.
- [ ] Expiry badges read **"as of trade date"** wording. With the sample data
      (trade `2010-08-20`, expiry `2010-09-10` = 21 days) there should be **no**
      badge — if every row is red "expired", the comparison is using wall clock.
- [ ] KPI row: Transactions `717`, pairs `5`, distinct clients `4`, and Fees showing
      a **negative** USD figure. No reconciliation warning (717 = 717).
- [ ] Source-file panel shows `sample-data/Input.txt`, a size, a modified time, an
      ingest time, and `717 / 0 / 0`. It must **not** show an absolute path.
- [ ] Theme toggle cycles Auto → Light → Dark, both modes are legible, and the
      choice survives a reload.
- [ ] Auto-refresh on: the "updated Ns ago" readout keeps resetting. Turn it off:
      the Refresh button enables and the readout ages.
- [ ] Stop processing-service (`docker compose stop processing-service`) with
      auto-refresh on: **the table stays on screen** with a stale badge, rather
      than blanking. Restart it and the badge clears.
- [ ] Downloaded CSV is byte-identical: `diff <(curl -s
      http://localhost:8080/api/v1/report/csv) sample-output/Output.csv` prints
      nothing.
- [ ] `curl -s -o /dev/null -w '%{http_code}' -X POST
      http://localhost:8080/api/v1/ingest` returns `404`.

- [ ] **Step 10: Update the READMEs**

In the root `README.md`, replace the `frontend` bullet under Status:

```markdown
- `frontend` — done: Tailwind-styled Angular UI showing the daily summary with
  per-field columns (17 available, 8 shown by default, choice persisted), client /
  account / product filters plus global search, sortable columns, a diverging
  net-quantity bar, source-file provenance, a KPI row with per-currency fee totals,
  light/dark themes, and 5-second auto-refresh with a manual fallback. A failed
  refresh keeps the last good data rather than blanking the table. See its
  [README](frontend/README.md) for usage.
```

In `frontend/README.md`, document: the two proxied upstreams (report →
processing-service 8082, `GET /api/v1/ingest/status` → ingestion-service 8081, and
that `POST /api/v1/ingest` is deliberately unreachable through this origin); the
`localStorage` keys `pfm.theme`, `pfm.autoRefresh`, `pfm.visibleColumns`; the
5-second poll interval and its hidden-tab pause; and that expiry badges are
measured against the row's last trade date rather than wall clock.

- [ ] **Step 11: Final full verification**

```bash
mvn -q test && (cd frontend && npm test && npm run build)
```

Expected: everything passes. State the actual result — do not claim success
without this output.

- [ ] **Step 12: Commit**

```bash
git add -A
git commit -m "feat(frontend): assemble the redesigned report page"
```

---

## Done when

- `mvn -q test` passes, including `FullPipelineGoldenTest` proving the CSV is
  byte-identical to `sample-output/Output.csv`.
- `npm test` and `npm run build` pass in `frontend/`.
- The Step 9 checklist is fully ticked, having started from
  `docker compose down -v --remove-orphans`.
- `POST /api/v1/ingest` through the frontend origin returns 404.
