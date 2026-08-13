package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.NetPosition;
import com.pfm.processing.ProcessingProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Clock;

@Configuration
@EnableKafkaStreams
public class AggregationTopology {

    public static final String NET_QUANTITY_STORE = "net-quantity-store";

    @Bean
    public KStream<String, FutureTransaction> netQuantityStream(StreamsBuilder streamsBuilder,
                                                                  ProcessingProperties properties) {
        return build(streamsBuilder, properties.topic(), Clock.systemUTC());
    }

    /**
     * @param clock supplies each aggregate's {@code lastUpdatedAt}. Injected rather than
     *              read from {@code Instant.now()} so tests can pin it; {@code Aggregator}
     *              has no {@code ProcessorContext} to read stream time from.
     */
    static KStream<String, FutureTransaction> build(StreamsBuilder streamsBuilder, String topic, Clock clock) {
        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DedupProcessor.STORE_NAME), Serdes.String(), Serdes.Long()));

        KStream<String, FutureTransaction> source = streamsBuilder.stream(
                topic, Consumed.with(Serdes.String(), TransactionSerde.instance()));

        // processValues, not process: see DedupProcessor. process() would force a
        // repartition topic between dedup and aggregation for no benefit.
        KStream<String, FutureTransaction> deduped =
                source.processValues(DedupProcessor::new, DedupProcessor.STORE_NAME);

        deduped.groupByKey(Grouped.with(Serdes.String(), TransactionSerde.instance()))
                .aggregate(
                        NetPosition::empty,
                        (key, transaction, position) -> position.plus(transaction, clock.instant()),
                        Materialized.<String, NetPosition, KeyValueStore<Bytes, byte[]>>as(NET_QUANTITY_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(NetPositionSerde.instance()));

        return deduped;
    }
}
