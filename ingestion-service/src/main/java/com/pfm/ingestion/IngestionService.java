package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.FutureTransactionParser;
import com.pfm.common.domain.ParseResult;
import com.pfm.common.fixedwidth.ParseError;
import com.pfm.ingestion.kafka.KafkaKeyBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class IngestionService {

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

        IngestionRegistry.CacheOutcome outcome = registry.getOrCompute(fingerprint, () -> runIngestion(path, fingerprint));
        return outcome.result().withCached(outcome.cached());
    }

    private IngestionResult runIngestion(Path path, String fingerprint) {
        List<String> lines;
        try {
            lines = Files.readAllLines(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        ParseResult parseResult = parser.parseAll(lines);

        List<ParseError> sendFailures = new ArrayList<>();
        int published = 0;
        for (FutureTransaction transaction : parseResult.records()) {
            String key = KafkaKeyBuilder.buildKey(transaction);
            try {
                kafkaTemplate.send(properties.topic(), key, transaction).get(10, TimeUnit.SECONDS);
                published++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sendFailures.add(new ParseError(-1, key, "Kafka send interrupted: " + e.getMessage()));
            } catch (ExecutionException | TimeoutException e) {
                sendFailures.add(new ParseError(-1, key, "Kafka send failed: " + e.getMessage()));
            }
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
