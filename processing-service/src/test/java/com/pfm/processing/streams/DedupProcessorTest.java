package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DedupProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, FutureTransaction> inputTopic;
    private TestOutputTopic<String, FutureTransaction> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DedupProcessor.STORE_NAME), Serdes.String(), Serdes.Long()));
        KStream<String, FutureTransaction> deduped = builder
                .stream("input-topic", Consumed.with(Serdes.String(), TransactionSerde.instance()))
                .process(DedupProcessor::new, DedupProcessor.STORE_NAME);
        deduped.to("output-topic", org.apache.kafka.streams.kstream.Produced.with(
                Serdes.String(), TransactionSerde.instance()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dedup-processor-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);

        inputTopic = driver.createInputTopic("input-topic", Serdes.String().serializer(),
                TransactionSerde.instance().serializer());
        outputTopic = driver.createOutputTopic("output-topic", Serdes.String().deserializer(),
                TransactionSerde.instance().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void firstOccurrenceOfATransactionIdIsForwarded() {
        FutureTransaction transaction = transaction(100, 30);

        pipeInput("key-1", transaction, "tx-1");

        List<FutureTransaction> forwarded = outputTopic.readValuesToList();
        assertEquals(1, forwarded.size());
        assertEquals(transaction, forwarded.get(0));
    }

    @Test
    void repeatedTransactionIdIsDroppedNotForwarded() {
        FutureTransaction transaction = transaction(100, 30);

        pipeInput("key-1", transaction, "tx-1");
        pipeInput("key-1", transaction, "tx-1");

        List<FutureTransaction> forwarded = outputTopic.readValuesToList();
        assertEquals(1, forwarded.size(), "the second delivery of the same transactionId must be dropped");
    }

    @Test
    void sameFieldValuesWithDifferentTransactionIdsAreBothForwarded() {
        FutureTransaction transaction = transaction(100, 30);

        pipeInput("key-1", transaction, "tx-1");
        pipeInput("key-1", transaction, "tx-2");

        List<FutureTransaction> forwarded = outputTopic.readValuesToList();
        assertEquals(2, forwarded.size(),
                "two distinct transactionIds must both be forwarded, even with identical field values");
    }

    private void pipeInput(String key, FutureTransaction transaction, String transactionId) {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("transactionId", transactionId.getBytes(StandardCharsets.UTF_8)));
        inputTopic.pipeInput(new TestRecord<>(key, transaction, headers, (Instant) null));
    }

    private FutureTransaction transaction(long quantityLong, long quantityShort) {
        return new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B',
                quantityLong, quantityShort,
                BigDecimal.valueOf(60, 2), "USD", 'D',
                BigDecimal.valueOf(30, 2), "USD", 'D',
                BigDecimal.ZERO, "JPY", 'D',
                LocalDate.of(2010, 8, 20), "001238", "0", "688032",
                BigDecimal.valueOf(925, 5), "TRDR12", "OPP0001", 'O');
    }
}
