package com.pfm.processing.report;

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

        ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        AggregationTopology.NET_QUANTITY_STORE, QueryableStoreTypes.keyValueStore()));

        List<ReportEntry> entries = new ArrayList<>();
        try (KeyValueIterator<String, Long> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Long> entry = iterator.next();
                entries.add(toReportEntry(entry.key, entry.value));
            }
        }
        entries.sort(Comparator.comparing(ReportEntry::clientInformation)
                .thenComparing(ReportEntry::productInformation));
        return entries;
    }

    private ReportEntry toReportEntry(String key, long netQuantity) {
        int separatorIndex = key.indexOf('|');
        String clientInformation = key.substring(0, separatorIndex);
        String productInformation = key.substring(separatorIndex + 1);
        return new ReportEntry(clientInformation, productInformation, netQuantity);
    }
}
