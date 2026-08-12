package com.pfm.processing.report;

import com.pfm.common.domain.NetPosition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReportServiceTest {

    private static NetPosition position(long netQuantity) {
        return new NetPosition(netQuantity, Math.max(netQuantity, 0), Math.max(-netQuantity, 0), 1,
                LocalDate.of(2010, 8, 20), LocalDate.of(2010, 8, 20),
                Instant.parse("2026-08-12T14:31:52Z"), Map.of());
    }

    @Test
    @SuppressWarnings("unchecked")
    void returnsEntriesSortedByClientThenProductInformation() {
        ReadOnlyKeyValueStore<String, NetPosition> store = mock(ReadOnlyKeyValueStore.class);
        when(store.all()).thenReturn(iteratorOver(List.of(
                KeyValue.pair("CL|4321|0002|0001|SGX|FU|NK|20100910", position(46L)),
                KeyValue.pair("CL|1234|0003|0001|CME|FU|NK.|20100910", position(-215L)),
                KeyValue.pair("CL|1234|0003|0001|CME|FU|N1|20100910", position(285L)),
                KeyValue.pair("CL|1234|0002|0001|SGX|FU|NK|20100910", position(-52L)))));

        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);

        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        ReportService service = new ReportService(factoryBean);
        List<ReportEntry> report = service.currentReport();

        assertEquals(4, report.size());
        assertEquals(List.of("CL123400020001", "CL123400030001", "CL123400030001", "CL432100020001"),
                report.stream().map(ReportEntry::clientInformation).toList());
        assertEquals(List.of("SGXFUNK20100910", "CMEFUN120100910", "CMEFUNK.20100910", "SGXFUNK20100910"),
                report.stream().map(ReportEntry::productInformation).toList());
        assertEquals(List.of(-52L, 285L, -215L, 46L),
                report.stream().map(ReportEntry::netQuantity).toList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void decomposesTheKeyIntoIndividualDimensionFields() {
        // Explicit NetPosition (not the position() helper) so grossLong is
        // independently verifiable: netQuantity=-215 with grossLong=285 can only
        // happen via offsetting activity, not via the helper's max(netQuantity, 0).
        NetPosition netPosition = new NetPosition(-215L, 285L, 500L, 12,
                LocalDate.of(2010, 8, 19), LocalDate.of(2010, 8, 20),
                Instant.parse("2026-08-12T14:31:52Z"), Map.of("USD", new BigDecimal("-0.90")));
        ReadOnlyKeyValueStore<String, NetPosition> store = mock(ReadOnlyKeyValueStore.class);
        when(store.all()).thenReturn(iteratorOver(List.of(
                KeyValue.pair("CL|1234|0003|0001|CME|FU|NK.|20100910", netPosition))));

        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);
        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        ReportEntry entry = new ReportService(factoryBean).currentReport().get(0);

        assertEquals("CL", entry.clientType());
        assertEquals("1234", entry.clientNumber());
        assertEquals("0003", entry.accountNumber());
        assertEquals("0001", entry.subaccountNumber());
        assertEquals("CME", entry.exchangeCode());
        assertEquals("FU", entry.productGroupCode());
        assertEquals("NK.", entry.symbol());
        assertEquals(LocalDate.of(2010, 9, 10), entry.expirationDate());
        assertEquals(285L, entry.grossLong());
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

    private static KeyValueIterator<String, NetPosition> iteratorOver(List<KeyValue<String, NetPosition>> entries) {
        Iterator<KeyValue<String, NetPosition>> delegate = entries.iterator();
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
            public KeyValue<String, NetPosition> next() {
                return delegate.next();
            }
        };
    }
}
