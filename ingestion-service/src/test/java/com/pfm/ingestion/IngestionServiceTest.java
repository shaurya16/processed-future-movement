package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.FutureTransactionParser;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
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

    @SuppressWarnings("unchecked")
    private void stubAllSendsSucceed() {
        when(kafkaTemplate.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(null));
    }

    /**
     * {@code KafkaTemplate} overloads both {@code send(ProducerRecord<K,V>)} and
     * {@code send(Message<?>)}; a raw {@code any(ProducerRecord.class)} matcher is ambiguous
     * between the two at compile time. Pinning the generic type via {@code ArgumentMatchers.any()}
     * resolves the overload unambiguously.
     */
    @SuppressWarnings("unchecked")
    private static ProducerRecord<String, FutureTransaction> anyProducerRecord() {
        return org.mockito.ArgumentMatchers.any();
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<ProducerRecord<String, FutureTransaction>> captureProducerRecords() {
        return ArgumentCaptor.forClass(ProducerRecord.class);
    }

    private List<String> transactionIdsOf(List<ProducerRecord<String, FutureTransaction>> records) {
        return records.stream()
                .map(r -> new String(r.headers().lastHeader("transactionId").value(), StandardCharsets.UTF_8))
                .toList();
    }

    @Test
    void ingestsFileAndPublishesEachParsedRecordKeyedByClientAndProduct() {
        stubAllSendsSucceed();

        IngestionResult result = service.ingest(false);

        assertEquals(3, result.totalLines());
        assertEquals(2, result.published());
        assertEquals(1, result.skipped());
        assertEquals(1, result.errors().size());
        assertEquals(3, result.errors().get(0).lineNumber());
        assertFalse(result.cached());
        verify(kafkaTemplate, times(2)).send(argThat((ProducerRecord<String, FutureTransaction> record) ->
                "future-transactions".equals(record.topic())
                        && "CL432100020001|SGXFUNK20100910".equals(record.key())));
    }

    @Test
    void publishesEachRecordWithADistinctTransactionIdHeaderDerivedFromContentHashAndLineNumber() {
        stubAllSendsSucceed();
        ArgumentCaptor<ProducerRecord<String, FutureTransaction>> captor = captureProducerRecords();

        service.ingest(false);

        verify(kafkaTemplate, times(2)).send(captor.capture());
        List<String> transactionIds = transactionIdsOf(captor.getAllValues());

        assertEquals(2, transactionIds.size());
        // small-sample.txt's first two lines are byte-identical FutureTransaction content;
        // these values must still differ because they're derived from (contentHash, lineNumber).
        assertEquals("e67676947c605bc5218aa1812fe5ec3c5e9d2ff34481343f2fc22633fc833413", transactionIds.get(0));
        assertEquals("cb418a2ad4a9f87431544d1ebd2d25405f7ee3ebb31972524abb8f7678459806", transactionIds.get(1));
    }

    @Test
    void transactionIdHeaderIsStableAcrossRepeatForcedIngestionOfUnchangedContent() {
        stubAllSendsSucceed();
        ArgumentCaptor<ProducerRecord<String, FutureTransaction>> captor = captureProducerRecords();

        service.ingest(true);
        service.ingest(true);

        verify(kafkaTemplate, times(4)).send(captor.capture());
        List<ProducerRecord<String, FutureTransaction>> sent = captor.getAllValues();

        List<String> firstCallIds = transactionIdsOf(sent.subList(0, 2));
        List<String> secondCallIds = transactionIdsOf(sent.subList(2, 4));
        assertEquals(firstCallIds, secondCallIds,
                "re-ingesting unchanged content via force=true must reproduce identical transaction IDs");
    }

    @Test
    void secondIngestOfSameFileIsServedFromCacheWithoutRepublishing() {
        stubAllSendsSucceed();

        IngestionResult first = service.ingest(false);
        IngestionResult second = service.ingest(false);

        assertFalse(first.cached());
        assertTrue(second.cached());
        assertEquals(first.published(), second.published());
        verify(kafkaTemplate, times(2)).send(anyProducerRecord());
    }

    @Test
    void forceBypassesCacheAndRepublishes() {
        stubAllSendsSucceed();

        service.ingest(false);
        IngestionResult forced = service.ingest(true);

        assertFalse(forced.cached());
        verify(kafkaTemplate, times(4)).send(anyProducerRecord());
    }

    @Test
    @SuppressWarnings("unchecked")
    void throwsKafkaPublishExceptionWhenAllSendsFail() {
        CompletableFuture<SendResult<String, FutureTransaction>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));
        when(kafkaTemplate.send(anyProducerRecord())).thenReturn(failed);

        KafkaPublishException exception = assertThrows(KafkaPublishException.class, () -> service.ingest(false));
        assertTrue(exception.getMessage().contains("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void abortsSendingAfterConsecutiveFailureThresholdToBoundWorstCaseBlockingTime() throws URISyntaxException {
        CompletableFuture<SendResult<String, FutureTransaction>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));
        when(kafkaTemplate.send(anyProducerRecord())).thenReturn(failed);

        Path fixture = Path.of(getClass().getClassLoader().getResource("many-valid-records-sample.txt").toURI());
        IngestionProperties properties = new IngestionProperties(fixture.toString(), "future-transactions");
        IngestionService manyRecordsService =
                new IngestionService(new FutureTransactionParser(), kafkaTemplate, new IngestionRegistry(), properties);

        // 6 valid records in the fixture, all sends fail: the service should stop after
        // MAX_CONSECUTIVE_SEND_FAILURES (5) rather than blocking up to 10s on every one.
        assertThrows(KafkaPublishException.class, () -> manyRecordsService.ingest(false));

        verify(kafkaTemplate, times(5)).send(anyProducerRecord());
    }

    @Test
    @SuppressWarnings("unchecked")
    void partialKafkaSendFailureIsNotCachedSoTheNextNonForcedCallRetries() {
        CompletableFuture<SendResult<String, FutureTransaction>> success = CompletableFuture.completedFuture(null);
        CompletableFuture<SendResult<String, FutureTransaction>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));

        // Each ingest() attempt over small-sample.txt has 2 valid records: alternate
        // success/failure so every attempt is a genuine partial failure (1 published, 1 failed).
        when(kafkaTemplate.send(anyProducerRecord())).thenReturn(success, failed, success, failed);

        IngestionResult first = service.ingest(false);
        assertEquals(1, first.published());
        assertFalse(first.cached());
        assertTrue(first.errors().stream().anyMatch(e -> e.lineNumber() == -1),
                "expected a Kafka send failure (lineNumber -1) among the errors");

        // Decision (finding #5): a result with un-retried Kafka send failures must NOT be
        // cached, because a subsequent non-forced call must retry the failed records rather
        // than silently replaying a known-partial ingestion as if it fully succeeded.
        IngestionResult second = service.ingest(false);
        assertFalse(second.cached(), "a partial-failure result must not be served from cache");
        assertEquals(1, second.published());

        verify(kafkaTemplate, times(4)).send(anyProducerRecord());
    }

    @Test
    void throwsIngestionFileNotFoundExceptionForMissingFile() {
        IngestionProperties missing = new IngestionProperties("does-not-exist.txt", "future-transactions");
        IngestionService missingFileService =
                new IngestionService(new FutureTransactionParser(), kafkaTemplate, new IngestionRegistry(), missing);

        assertThrows(IngestionFileNotFoundException.class, () -> missingFileService.ingest(false));
    }
}
