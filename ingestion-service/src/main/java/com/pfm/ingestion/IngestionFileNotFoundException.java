package com.pfm.ingestion;

import java.nio.file.Path;

public class IngestionFileNotFoundException extends RuntimeException {

    public IngestionFileNotFoundException(Path path) {
        super("Ingestion file not found: " + path.toAbsolutePath());
    }
}
