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
