package com.pfm.ingestion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ContentHash {

    private ContentHash() {
    }

    public static String compute(Path path) throws IOException {
        return Sha256.hexDigest(Files.readAllBytes(path));
    }
}
