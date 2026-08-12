package com.pfm.ingestion;

import java.time.Instant;

/**
 * Provenance for the file the report is built from.
 *
 * <p>{@code configuredPath} is the {@code ingestion.file-path} config value as
 * written, deliberately not the resolved absolute path — it is what an operator
 * wants to verify and it discloses nothing about the container's filesystem.
 *
 * <p>All run fields are null until an ingestion has actually happened; that is a
 * normal state, not an error, and the endpoint still returns 200.
 */
public record IngestionStatus(
        String configuredPath,
        boolean fileExists,
        Long fileSizeBytes,
        Instant fileLastModified,
        Instant lastIngestAt,
        String fingerprint,
        Integer totalLines,
        Integer published,
        Integer skipped,
        Integer errorCount) {
}
