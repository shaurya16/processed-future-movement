package com.pfm.ingestion;

import java.nio.charset.StandardCharsets;

public final class TransactionIdBuilder {

    private TransactionIdBuilder() {
    }

    public static String build(String contentHash, int lineNumber) {
        String basis = contentHash + ":" + lineNumber;
        return Sha256.hexDigest(basis.getBytes(StandardCharsets.UTF_8));
    }
}
