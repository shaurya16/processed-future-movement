package com.pfm.processing.report;

import com.pfm.processing.streams.AggregationTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReportServiceTest {

    @Test
    @SuppressWarnings("unchecked")
    void returnsEntriesSortedByClientThenProductInformation() {
        ReadOnlyKeyValueStore<String, Long> store = mock(ReadOnlyKeyValueStore.class);
        when(store.all()).thenReturn(iteratorOver(List.of(
                KeyValue.pair("CL432100020001|SGXFUNK20100910", 46L),
                KeyValue.pair("CL123400030001|CMEFUNK.20100910", -215L),
                KeyValue.pair("CL123400030001|CMEFUN120100910", 285L),
                KeyValue.pair("CL123400020001|SGXFUNK20100910", -52L))));

        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);

        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        ReportService service = new ReportService(factoryBean);
        List<ReportEntry> report = service.currentReport();

        assertEquals(4, report.size());
        assertEquals(new ReportEntry("CL123400020001", "SGXFUNK20100910", -52L), report.get(0));
        assertEquals(new ReportEntry("CL123400030001", "CMEFUN120100910", 285L), report.get(1));
        assertEquals(new ReportEntry("CL123400030001", "CMEFUNK.20100910", -215L), report.get(2));
        assertEquals(new ReportEntry("CL432100020001", "SGXFUNK20100910", 46L), report.get(3));
    }

    @Test
    void throwsStoreNotReadyExceptionWhenKafkaStreamsIsNotRunning() {
        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);

        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        ReportService service = new ReportService(factoryBean);

        assertThrows(StoreNotReadyException.class, service::currentReport);
    }

    @Test
    void throwsStoreNotReadyExceptionWhenKafkaStreamsHasNotStartedYet() {
        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(null);

        ReportService service = new ReportService(factoryBean);

        assertThrows(StoreNotReadyException.class, service::currentReport);
    }

    private static KeyValueIterator<String, Long> iteratorOver(List<KeyValue<String, Long>> entries) {
        Iterator<KeyValue<String, Long>> delegate = entries.iterator();
        return new KeyValueIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public String peekNextKey() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public KeyValue<String, Long> next() {
                return delegate.next();
            }
        };
    }
}
