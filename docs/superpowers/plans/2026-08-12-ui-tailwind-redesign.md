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
| Modify `.../IngestionRegistry.java` | Record `lastIngest` (only on actual compute). |
| Create `.../ClockConfig.java` | `Clock` bean so the registry's timestamp is testable. |
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

**Config** — `frontend/nginx.conf.template`, `frontend/Dockerfile`,
`frontend/proxy.conf.json`, `docker-compose.yml`, `k8s/frontend.yaml`.

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
