package com.pfm.processing.report;

public class StoreNotReadyException extends RuntimeException {

    public StoreNotReadyException() {
        super("processing-service's aggregate store is not ready yet "
                + "(Kafka Streams is still starting or rebalancing)");
    }
}
