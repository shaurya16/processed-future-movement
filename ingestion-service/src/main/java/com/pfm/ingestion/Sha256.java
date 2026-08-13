package com.pfm.ingestion;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Lowercase-hex SHA-256, shared by the two things that need it: {@link ContentHash}
 * (file identity) and {@link TransactionIdBuilder} (per-record dedup id).
 *
 * <p>Both previously carried their own byte-identical copy of the digest-and-hex code
 * and disagreed on how to report the impossible case.
 */
final class Sha256 {

    private Sha256() {
    }

    static String hexDigest(byte[] input) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is mandatory on every JDK security provider, so this can only
            // happen if the JVM itself is misconfigured -- not an I/O condition.
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
