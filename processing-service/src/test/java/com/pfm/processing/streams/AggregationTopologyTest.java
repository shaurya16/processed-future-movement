package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.NetPosition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class AggregationTopologyTest {

    private static final String TOPIC = "future-transactions";
    private static final String KEY = "CL432100020001|SGXFUNK20100910";
    private static final Instant NOW = Instant.parse("2026-08-12T14:31:52Z");
    private static final Clock FIXED_CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private TopologyTestDriver driver;
    private TestInputTopic<String, FutureTransaction> inputTopic;

    @Test
    void topologyIsASingleSubTopologyWithNoRepartition() {
        StreamsBuilder builder = new StreamsBuilder();
        AggregationTopology.build(builder, TOPIC, FIXED_CLOCK);
        TopologyDescription description = builder.build().describe();

        // Dedup never changes the key, so aggregation is partition-local and nothing
        // needs to cross the broker between the two. KStream.process() would silently
        // reintroduce a repartition topic here (it always marks its output as needing
        // one); processValues() does not. docs/architecture.md states this property,
        // so the docs are wrong the moment this test fails.
        assertEquals(1, description.subtopologies().size(),
                "a repartition splits the topology in two; dedup must not force one");
        assertFalse(description.toString().contains("repartition"),
                "topology must not contain a repartition topic:\n" + description);
    }

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        AggregationTopology.build(builder, TOPIC, FIXED_CLOCK);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregation-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);

        inputTopic = driver.createInputTopic(TOPIC, Serdes.String().serializer(), TransactionSerde.create().serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void aggregatesNetQuantityPerKeyAcrossMultipleRecords() {
        pipeInput(KEY, transaction(100, 30), "tx-1"); // net +70
        pipeInput(KEY, transaction(50, 80), "tx-2");   // net -30

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(40L, store.get(KEY).netQuantity());
    }

    @Test
    void duplicateTransactionIdDoesNotDoubleCountInTheAggregate() {
        FutureTransaction transaction = transaction(100, 30); // net +70

        pipeInput(KEY, transaction, "tx-1");
        pipeInput(KEY, transaction, "tx-1"); // retried/re-published duplicate

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(70L, store.get(KEY).netQuantity());
        assertEquals(1, store.get(KEY).tradeCount());
    }

    @Test
    void twoLegitimateTransactionsWithIdenticalFieldValuesBothCount() {
        FutureTransaction transaction = transaction(100, 30); // net +70 each

        pipeInput(KEY, transaction, "tx-1");
        pipeInput(KEY, transaction, "tx-2"); // different transactionId: a real second trade

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(140L, store.get(KEY).netQuantity());
    }

    @Test
    void aNeverSeenKeyHasNoEntryInTheStore() {
        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertNull(store.get("no-such-key"));
    }

    @Test
    void tracksGrossQuantitiesAndTradeCountAlongsideNet() {
        pipeInput(KEY, transaction(500, 0), "tx-1");
        pipeInput(KEY, transaction(0, 500), "tx-2");

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        NetPosition position = store.get(KEY);

        assertEquals(0L, position.netQuantity());
        assertEquals(500L, position.grossLong());
        assertEquals(500L, position.grossShort());
        assertEquals(2, position.tradeCount());
    }

    @Test
    void stampsLastUpdatedAtFromTheInjectedClock() {
        pipeInput(KEY, transaction(100, 30), "tx-1");

        KeyValueStore<String, NetPosition> store =
                driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(NOW, store.get(KEY).lastUpdatedAt());
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
