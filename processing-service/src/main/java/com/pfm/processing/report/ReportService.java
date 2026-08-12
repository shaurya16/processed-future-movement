package com.pfm.processing.report;

import com.pfm.common.domain.NetPosition;
import com.pfm.common.domain.ReportKey;
import com.pfm.processing.streams.AggregationTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class ReportService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public ReportService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public List<ReportEntry> currentReport() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            throw new StoreNotReadyException();
        }

        ReadOnlyKeyValueStore<String, NetPosition> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        AggregationTopology.NET_QUANTITY_STORE, QueryableStoreTypes.keyValueStore()));

        List<ReportEntry> entries = new ArrayList<>();
        try (KeyValueIterator<String, NetPosition> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, NetPosition> entry = iterator.next();
                entries.add(ReportEntry.of(ReportKey.decode(entry.key), entry.value));
            }
        }
        entries.sort(Comparator.comparing(ReportEntry::clientInformation)
                .thenComparing(ReportEntry::productInformation));
        return entries;
    }
}
