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
  to `sample-output/Output.csv`: 246 bytes, header
  `Client_Information,Product_Information,Total_Transaction_Amount`, one row per
  entry, trailing `\n`. Task 6 locks this with a golden test.
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

- [ ] **Step 5: Commit**

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

- [ ] **Step 6: Commit**

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

- [ ] **Step 3: Run the golden test**

Run: `mvn -q -pl processing-service -am test -Dtest=FullPipelineGoldenTest`
Expected: PASS. This is the single most important verification in the plan — it
proves the key and aggregate rewrites left the reported numbers and row order
untouched. Requires Docker (Testcontainers).

If it fails on row *order*, check that `ReportService` still sorts by
`clientInformation` then `productInformation`. If it fails on *values*, the
`plus` accumulator's `netQuantity` arithmetic is wrong.

- [ ] **Step 4: Document the teardown in the processing-service README**

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
docker compose down -v                          # containerised paths
rm -rf /tmp/kafka-streams/processing-service    # broker-only loop, host-side state
```

Kafka Streams defaults `auto.offset.reset` to `earliest` (unlike a plain
consumer, which defaults to `latest`), so after teardown the topic replays from
the beginning and the store rebuilds with the new key format automatically.
```

- [ ] **Step 5: Run the full backend suite**

Run: `mvn -q test`
Expected: PASS across `common`, `ingestion-service`, `processing-service`.

- [ ] **Step 6: Commit**

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
              value: http://ingestion-service:8081
```

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
