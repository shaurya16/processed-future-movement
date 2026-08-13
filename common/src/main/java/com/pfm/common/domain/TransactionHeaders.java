package com.pfm.common.domain;

/**
 * Kafka record header names shared by the producer (ingestion-service) and the
 * consumer (processing-service).
 *
 * <p>Declared here for the same reason {@link ReportKey} is: the header name is half
 * of the message contract, and the two services cannot be allowed to drift apart on
 * it. Drift fails silently rather than loudly — {@code DedupProcessor} logs a warning
 * and forwards the record anyway, so deduplication just stops working and re-ingesting
 * a file doubles every total in the report.
 */
public final class TransactionHeaders {

    /** Content-derived id ({@code sha256(contentHash + ":" + lineNumber)}) used to deduplicate. */
    public static final String TRANSACTION_ID = "transactionId";

    private TransactionHeaders() {
    }
}
