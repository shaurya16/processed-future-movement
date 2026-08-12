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
