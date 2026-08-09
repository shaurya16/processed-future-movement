package com.pfm.ingestion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class ContentHash {

    private ContentHash() {
    }

    public static String compute(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        return hex(sha256(bytes));
    }

    private static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a mandatory algorithm on every JDK security provider; this
            // can only happen if the JVM itself is misconfigured.
            throw new UncheckedIOException(new IOException("SHA-256 not available", e));
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
