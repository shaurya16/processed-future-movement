package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;

import java.util.List;

public record IngestionResult(
        String fingerprint,
        int totalLines,
        int published,
        int skipped,
        List<ParseError> errors,
        boolean cached
) {
    public IngestionResult withCached(boolean cached) {
        return new IngestionResult(fingerprint, totalLines, published, skipped, errors, cached);
    }
}
