package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.FutureTransactionParser;
import com.pfm.common.domain.ParseResult;
import com.pfm.common.domain.ParsedRecord;
import com.pfm.common.fixedwidth.ParseError;
import com.pfm.ingestion.kafka.KafkaKeyBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class IngestionService {

    /**
     * Bounds worst-case blocking time when the broker is unreachable/degraded: after this
     * many consecutive Kafka send failures we stop attempting further sends for this batch
     * rather than blocking up to 10s per remaining record (which for a 717-record file could
     * hold an HTTP request open for close to two hours).
     */
    private static final int MAX_CONSECUTIVE_SEND_FAILURES = 5;

    private final FutureTransactionParser parser;
    private final KafkaTemplate<String, FutureTransaction> kafkaTemplate;
    private final IngestionRegistry registry;
    private final IngestionProperties properties;

    public IngestionService(FutureTransactionParser parser,
                             KafkaTemplate<String, FutureTransaction> kafkaTemplate,
                             IngestionRegistry registry,
                             IngestionProperties properties) {
        this.parser = parser;
        this.kafkaTemplate = kafkaTemplate;
        this.registry = registry;
        this.properties = properties;
    }

    public IngestionResult ingest(boolean force) {
        Path path = Path.of(properties.filePath());
        if (!Files.exists(path)) {
            throw new IngestionFileNotFoundException(path);
        }

        String fingerprint;
        try {
            fingerprint = FileFingerprint.compute(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (force) {
            return registry.forceCompute(fingerprint, () -> runIngestion(path, fingerprint));
        }

        IngestionRegistry.CacheOutcome outcome = registry.getOrCompute(
                fingerprint,
                () -> runIngestion(path, fingerprint),
                IngestionService::hasNoKafkaSendFailures);
        return outcome.result().withCached(outcome.cached());
    }

    /**
     * A result is only safe to cache (and serve to future non-forced requests without
     * republishing) when every parsed record actually made it to Kafka. Parse errors are
     * deterministic for a given file and caching them is fine; Kafka send failures (tagged
     * with lineNumber == -1, see runIngestion below) are transient infrastructure failures,
     * and caching a batch that has known un-retried send failures would mean a financial
     * pipeline silently treats a partially-ingested file as fully ingested forever. See
     * finding #5 of the final-review fix wave for the full reasoning.
     */
    private static boolean hasNoKafkaSendFailures(IngestionResult result) {
        return result.errors().stream().noneMatch(error -> error.lineNumber() == -1);
    }

    private IngestionResult runIngestion(Path path, String fingerprint) {
        List<String> lines;
        String contentHash;
        try {
            lines = Files.readAllLines(path);
            contentHash = ContentHash.compute(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        ParseResult parseResult = parser.parseAll(lines);

        List<ParseError> sendFailures = new ArrayList<>();
        List<ParsedRecord> records = parseResult.records();
        int published = 0;
        int consecutiveFailures = 0;
        int attempted = 0;
        for (ParsedRecord parsedRecord : records) {
            attempted++;
            FutureTransaction transaction = parsedRecord.transaction();
            String key = KafkaKeyBuilder.buildKey(transaction);
            String transactionId = TransactionIdBuilder.build(contentHash, parsedRecord.lineNumber());
            ProducerRecord<String, FutureTransaction> producerRecord = new ProducerRecord<>(
                    properties.topic(), null, key, transaction,
                    List.of(new RecordHeader("transactionId", transactionId.getBytes(StandardCharsets.UTF_8))));
            try {
                kafkaTemplate.send(producerRecord).get(10, TimeUnit.SECONDS);
                published++;
                consecutiveFailures = 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sendFailures.add(new ParseError(-1, key, "Kafka send interrupted: " + e.getMessage()));
                consecutiveFailures++;
            } catch (ExecutionException | TimeoutException e) {
                sendFailures.add(new ParseError(-1, key, "Kafka send failed: " + e.getMessage()));
                consecutiveFailures++;
            }
            if (consecutiveFailures >= MAX_CONSECUTIVE_SEND_FAILURES) {
                break;
            }
        }

        if (attempted < records.size()) {
            int notAttempted = records.size() - attempted;
            sendFailures.add(new ParseError(-1, "n/a",
                    "Aborted after " + MAX_CONSECUTIVE_SEND_FAILURES
                            + " consecutive Kafka send failures; " + notAttempted
                            + " record(s) not attempted"));
        }

        List<ParseError> allErrors = new ArrayList<>(parseResult.errors());
        allErrors.addAll(sendFailures);

        if (!parseResult.records().isEmpty() && published == 0) {
            throw new KafkaPublishException(
                    "All " + parseResult.records().size() + " parsed records failed to publish to topic '"
                            + properties.topic() + "'",
                    allErrors);
        }

        return new IngestionResult(fingerprint, lines.size(), published, lines.size() - published, allErrors, false);
    }
}
