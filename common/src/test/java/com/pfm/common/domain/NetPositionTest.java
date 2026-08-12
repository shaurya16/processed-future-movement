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
