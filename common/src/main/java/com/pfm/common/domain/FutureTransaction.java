package com.pfm.common.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/** Fully typed future-transaction record: every raw field converted to its real type. */
public record FutureTransaction(
        String recordCode,
        String clientType,
        String clientNumber,
        String accountNumber,
        String subaccountNumber,
        String oppositePartyCode,
        String productGroupCode,
        String exchangeCode,
        String symbol,
        LocalDate expirationDate,
        String currencyCode,
        String movementCode,
        char buySellCode,
        long quantityLong,
        long quantityShort,
        BigDecimal exchBrokerFee,
        String exchBrokerFeeCurrency,
        BigDecimal clearingFee,
        String clearingFeeCurrency,
        BigDecimal commission,
        String commissionCurrency,
        LocalDate transactionDate,
        String futureReference,
        String ticketNumber,
        String externalNumber,
        BigDecimal transactionPrice,
        String traderInitials,
        String oppositeTraderId,
        char openCloseCode
) {
}
