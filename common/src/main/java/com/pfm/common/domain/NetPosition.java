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
