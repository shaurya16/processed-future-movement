package com.pfm.ingestion.kafka;

import com.pfm.common.domain.FutureTransaction;

public final class KafkaKeyBuilder {

    private KafkaKeyBuilder() {
    }

    public static String buildKey(FutureTransaction transaction) {
        String clientInformation = transaction.clientType() + transaction.clientNumber()
                + transaction.accountNumber() + transaction.subaccountNumber();
        String productInformation = transaction.exchangeCode() + transaction.productGroupCode()
                + transaction.symbol() + transaction.expirationDate();
        return clientInformation + "|" + productInformation;
    }
}
