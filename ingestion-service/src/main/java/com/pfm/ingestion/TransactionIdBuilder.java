package com.pfm.ingestion;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class TransactionIdBuilder {

    private TransactionIdBuilder() {
    }

    public static String build(String contentHash, int lineNumber) {
        String basis = contentHash + ":" + lineNumber;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(basis.getBytes(StandardCharsets.UTF_8));
            return hex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
