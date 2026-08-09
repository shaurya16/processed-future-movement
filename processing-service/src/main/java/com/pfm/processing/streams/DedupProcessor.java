package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class DedupProcessor implements Processor<String, FutureTransaction, String, FutureTransaction> {

    public static final String STORE_NAME = "seen-transaction-ids";
    public static final String TRANSACTION_ID_HEADER = "transactionId";

    private static final Logger log = LoggerFactory.getLogger(DedupProcessor.class);

    private ProcessorContext<String, FutureTransaction> context;
    private KeyValueStore<String, Long> seenTransactionIds;

    @Override
    public void init(ProcessorContext<String, FutureTransaction> context) {
        this.context = context;
        this.seenTransactionIds = context.getStateStore(STORE_NAME);
    }

    @Override
    public void process(Record<String, FutureTransaction> record) {
        String transactionId = extractTransactionId(record);
        if (transactionId == null) {
            log.warn("Record for key {} has no transactionId header; forwarding without dedup tracking", record.key());
            context.forward(record);
            return;
        }
        if (seenTransactionIds.get(transactionId) != null) {
            return;
        }
        seenTransactionIds.put(transactionId, context.currentSystemTimeMs());
        context.forward(record);
    }

    private String extractTransactionId(Record<String, FutureTransaction> record) {
        Header header = record.headers().lastHeader(TRANSACTION_ID_HEADER);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8);
    }
}
