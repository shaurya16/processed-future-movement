package com.pfm.ingestion.kafka;

import com.pfm.common.domain.FutureTransaction;

import java.time.format.DateTimeFormatter;

public final class KafkaKeyBuilder {

    private KafkaKeyBuilder() {
    }

    public static String buildKey(FutureTransaction transaction) {
        String clientInformation = transaction.clientType() + transaction.clientNumber()
                + transaction.accountNumber() + transaction.subaccountNumber();
        String productInformation = transaction.exchangeCode() + transaction.productGroupCode()
                + transaction.symbol() + DateTimeFormatter.BASIC_ISO_DATE.format(transaction.expirationDate());
        return clientInformation + "|" + productInformation;
    }
}
