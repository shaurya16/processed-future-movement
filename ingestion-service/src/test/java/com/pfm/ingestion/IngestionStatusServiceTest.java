package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngestionStatusServiceTest {

    private static final Instant T1 = Instant.parse("2026-08-12T14:31:52Z");

    @Test
    void reportsFileMetadataAndNullRunFieldsBeforeAnyIngestion(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("Input.txt");
        Files.writeString(file, "some content", StandardCharsets.UTF_8);
        IngestionStatusService service = new IngestionStatusService(
                new IngestionProperties(file.toString(), "future-transactions"),
                new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC)));

        IngestionStatus status = service.currentStatus();

        assertTrue(status.fileExists());
        assertEquals(12L, status.fileSizeBytes());
        assertEquals(Files.getLastModifiedTime(file).toInstant(), status.fileLastModified());
        // Never ingested is a normal state, not an error.
        assertNull(status.lastIngestAt());
        assertNull(status.fingerprint());
        assertNull(status.totalLines());
        assertNull(status.published());
        assertNull(status.skipped());
        assertNull(status.errorCount());
    }

    @Test
    void reportsTheConfiguredPathVerbatimAndNotAnAbsolutePath() {
        // The controller strips absolute paths from error responses to avoid
        // advertising container filesystem layout; this endpoint honours that.
        IngestionStatusService service = new IngestionStatusService(
                new IngestionProperties("sample-data/Input.txt", "future-transactions"),
                new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC)));

        assertEquals("sample-data/Input.txt", service.currentStatus().configuredPath());
    }

    @Test
    void reportsFileAbsentWithoutFailingWhenThePathDoesNotExist(@TempDir Path dir) {
        IngestionStatusService service = new IngestionStatusService(
                new IngestionProperties(dir.resolve("nope.txt").toString(), "future-transactions"),
                new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC)));

        IngestionStatus status = service.currentStatus();

        assertFalse(status.fileExists());
        assertNull(status.fileSizeBytes());
        assertNull(status.fileLastModified());
    }

    @Test
    void reportsCountsAndTimestampAfterAnIngestion(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("Input.txt");
        Files.writeString(file, "x", StandardCharsets.UTF_8);
        IngestionRegistry registry = new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC));
        registry.getOrCompute("fp-1", () -> new IngestionResult("fp-1", 717, 715, 2,
                List.of(new ParseError(3, "bad line", "not numeric")), false));

        IngestionStatus status = new IngestionStatusService(
                new IngestionProperties(file.toString(), "future-transactions"), registry).currentStatus();

        assertEquals(T1, status.lastIngestAt());
        assertEquals("fp-1", status.fingerprint());
        assertEquals(717, status.totalLines());
        assertEquals(715, status.published());
        assertEquals(2, status.skipped());
        // The count only — raw lines contain client data and are never exposed.
        assertEquals(1, status.errorCount());
    }
}
