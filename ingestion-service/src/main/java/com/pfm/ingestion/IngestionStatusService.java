package com.pfm.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;

@Service
public class IngestionStatusService {

    private static final Logger log = LoggerFactory.getLogger(IngestionStatusService.class);

    private final IngestionProperties properties;
    private final IngestionRegistry registry;

    public IngestionStatusService(IngestionProperties properties, IngestionRegistry registry) {
        this.properties = properties;
        this.registry = registry;
    }

    public IngestionStatus currentStatus() {
        Path path = Path.of(properties.filePath());
        boolean exists = Files.exists(path);

        Long sizeBytes = null;
        Instant lastModified = null;
        if (exists) {
            try {
                sizeBytes = Files.size(path);
                lastModified = Files.getLastModifiedTime(path).toInstant();
            } catch (IOException e) {
                // Readable-then-unreadable is a race, not a client error: report the
                // file as present with unknown metadata rather than failing the call.
                log.warn("Could not read metadata for ingestion file: {}", e.getMessage());
            }
        }

        Optional<IngestionRegistry.LastIngest> last = registry.lastIngest();
        return new IngestionStatus(
                properties.filePath(),
                exists,
                sizeBytes,
                lastModified,
                last.map(IngestionRegistry.LastIngest::at).orElse(null),
                last.map(l -> l.result().fingerprint()).orElse(null),
                last.map(l -> l.result().totalLines()).orElse(null),
                last.map(l -> l.result().published()).orElse(null),
                last.map(l -> l.result().skipped()).orElse(null),
                last.map(l -> l.result().errors().size()).orElse(null));
    }
}
