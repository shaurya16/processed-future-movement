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
