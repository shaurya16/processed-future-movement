package com.pfm.ingestion;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TransactionIdBuilderTest {

    private static final String CONTENT_HASH =
            "273e42bc7eb45afc4a54e3b77b251dd59cbfd48bb1bebd6842eb6f59504992a9";

    @Test
    void buildsTheKnownTransactionIdForLine1() {
        assertEquals("e67676947c605bc5218aa1812fe5ec3c5e9d2ff34481343f2fc22633fc833413",
                TransactionIdBuilder.build(CONTENT_HASH, 1));
    }

    @Test
    void buildsTheKnownTransactionIdForLine2() {
        assertEquals("cb418a2ad4a9f87431544d1ebd2d25405f7ee3ebb31972524abb8f7678459806",
                TransactionIdBuilder.build(CONTENT_HASH, 2));
    }

    @Test
    void differentLineNumbersProduceDifferentIdsEvenForTheSameContentHash() {
        assertNotEquals(TransactionIdBuilder.build(CONTENT_HASH, 1), TransactionIdBuilder.build(CONTENT_HASH, 2));
    }

    @Test
    void sameContentHashAndLineNumberAlwaysProduceTheSameId() {
        assertEquals(TransactionIdBuilder.build(CONTENT_HASH, 5), TransactionIdBuilder.build(CONTENT_HASH, 5));
    }

    @Test
    void differentContentHashesProduceDifferentIdsForTheSameLineNumber() {
        String otherHash = "0000000000000000000000000000000000000000000000000000000000000000".substring(0, 64);
        assertNotEquals(TransactionIdBuilder.build(CONTENT_HASH, 1), TransactionIdBuilder.build(otherHash, 1));
    }
}
