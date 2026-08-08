package com.pfm.ingestion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class FileFingerprintTest {

    @Test
    void sameFileProducesSameFingerprintAcrossCalls(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("input.txt");
        Files.writeString(file, "hello");

        String first = FileFingerprint.compute(file);
        String second = FileFingerprint.compute(file);

        assertEquals(first, second);
    }

    @Test
    void differentSizeProducesDifferentFingerprint(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("input.txt");
        Files.writeString(file, "hello");
        String before = FileFingerprint.compute(file);

        Files.writeString(file, "hello world, now longer");
        String after = FileFingerprint.compute(file);

        assertNotEquals(before, after);
    }

    @Test
    void differentLastModifiedProducesDifferentFingerprint(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("input.txt");
        Files.writeString(file, "hello");
        String before = FileFingerprint.compute(file);

        Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() + 60_000));
        String after = FileFingerprint.compute(file);

        assertNotEquals(before, after);
    }
}
