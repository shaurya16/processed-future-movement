package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.FutureTransactionParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class IngestionServiceTest {

    private KafkaTemplate<String, FutureTransaction> kafkaTemplate;
    private IngestionService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws URISyntaxException {
        kafkaTemplate = mock(KafkaTemplate.class);
        Path fixture = Path.of(getClass().getClassLoader().getResource("small-sample.txt").toURI());
        IngestionProperties properties = new IngestionProperties(fixture.toString(), "future-transactions");
        service = new IngestionService(new FutureTransactionParser(), kafkaTemplate, new IngestionRegistry(), properties);
    }

    @Test
    void ingestsFileAndPublishesEachParsedRecordKeyedByClientAndProduct() {
        when(kafkaTemplate.send(anyString(), anyString(), any(FutureTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        IngestionResult result = service.ingest(false);

        assertEquals(3, result.totalLines());
        assertEquals(2, result.published());
        assertEquals(1, result.skipped());
        assertEquals(1, result.errors().size());
        assertEquals(3, result.errors().get(0).lineNumber());
        assertFalse(result.cached());
        verify(kafkaTemplate, times(2))
                .send(eq("future-transactions"), eq("CL432100020001|SGXFUNK2010-09-10"), any(FutureTransaction.class));
    }

    @Test
    void secondIngestOfSameFileIsServedFromCacheWithoutRepublishing() {
        when(kafkaTemplate.send(anyString(), anyString(), any(FutureTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        IngestionResult first = service.ingest(false);
        IngestionResult second = service.ingest(false);

        assertFalse(first.cached());
        assertTrue(second.cached());
        assertEquals(first.published(), second.published());
        verify(kafkaTemplate, times(2)).send(anyString(), anyString(), any(FutureTransaction.class));
    }

    @Test
    void forceBypassesCacheAndRepublishes() {
        when(kafkaTemplate.send(anyString(), anyString(), any(FutureTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.ingest(false);
        IngestionResult forced = service.ingest(true);

        assertFalse(forced.cached());
        verify(kafkaTemplate, times(4)).send(anyString(), anyString(), any(FutureTransaction.class));
    }

    @Test
    void throwsKafkaPublishExceptionWhenAllSendsFail() {
        CompletableFuture<SendResult<String, FutureTransaction>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));
        when(kafkaTemplate.send(anyString(), anyString(), any(FutureTransaction.class))).thenReturn(failed);

        KafkaPublishException exception = assertThrows(KafkaPublishException.class, () -> service.ingest(false));
        assertTrue(exception.getMessage().contains("2"));
    }

    @Test
    void throwsIngestionFileNotFoundExceptionForMissingFile() {
        IngestionProperties missing = new IngestionProperties("does-not-exist.txt", "future-transactions");
        IngestionService missingFileService =
                new IngestionService(new FutureTransactionParser(), kafkaTemplate, new IngestionRegistry(), missing);

        assertThrows(IngestionFileNotFoundException.class, () -> missingFileService.ingest(false));
    }
}
