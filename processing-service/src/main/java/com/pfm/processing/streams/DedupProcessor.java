package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.TransactionHeaders;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Drops records whose {@code transactionId} header has been seen before.
 *
 * <p>Deliberately a {@link FixedKeyProcessor} rather than a {@code Processor}: this never
 * changes the key, and {@code KStream.process} unconditionally marks its output stream as
 * requiring a repartition, which would split the topology and push every deduped record
 * back through a repartition topic before the aggregation. {@code processValues} preserves
 * the key-safety guarantee at the type level, so the whole pipeline stays partition-local
 * in one sub-topology.
 */
public class DedupProcessor implements FixedKeyProcessor<String, FutureTransaction, FutureTransaction> {

    public static final String STORE_NAME = "seen-transaction-ids";

    private static final Logger log = LoggerFactory.getLogger(DedupProcessor.class);

    private FixedKeyProcessorContext<String, FutureTransaction> context;
    private KeyValueStore<String, Long> seenTransactionIds;

    @Override
    public void init(FixedKeyProcessorContext<String, FutureTransaction> context) {
        this.context = context;
        this.seenTransactionIds = context.getStateStore(STORE_NAME);
    }

    @Override
    public void process(FixedKeyRecord<String, FutureTransaction> record) {
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

    private String extractTransactionId(FixedKeyRecord<String, FutureTransaction> record) {
        Header header = record.headers().lastHeader(TransactionHeaders.TRANSACTION_ID);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8);
    }
}
