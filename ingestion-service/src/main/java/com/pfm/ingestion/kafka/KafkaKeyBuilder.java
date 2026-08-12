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
