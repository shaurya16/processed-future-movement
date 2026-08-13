package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.serialization.Serde;

/** JSON serde for the inbound transaction, as published by ingestion-service. */
public final class TransactionSerde {

    private TransactionSerde() {
    }

    /** A new serde per call — named create(), not instance(), because it allocates. */
    public static Serde<FutureTransaction> create() {
        return JsonSerdes.create(FutureTransaction.class);
    }
}
