package com.pfm.ingestion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class FileFingerprint {

    private FileFingerprint() {
    }

    public static String compute(Path path) throws IOException {
        long size = Files.size(path);
        long lastModifiedMillis = Files.getLastModifiedTime(path).toMillis();
        return path.toAbsolutePath() + "|" + size + "|" + lastModifiedMillis;
    }
}
