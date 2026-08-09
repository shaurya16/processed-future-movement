package com.pfm.ingestion;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ContentHashTest {

    @Test
    void computesTheKnownSha256HexDigestOfTheSmallSampleFixture() throws URISyntaxException, IOException {
        Path fixture = Path.of(getClass().getClassLoader().getResource("small-sample.txt").toURI());

        String hash = ContentHash.compute(fixture);

        assertEquals("273e42bc7eb45afc4a54e3b77b251dd59cbfd48bb1bebd6842eb6f59504992a9", hash);
        assertEquals(64, hash.length());
    }

    @Test
    void sameFileContentProducesTheSameHash() throws IOException {
        Path a = Files.createTempFile("content-hash-a", ".txt");
        Path b = Files.createTempFile("content-hash-b", ".txt");
        Files.writeString(a, "identical content");
        Files.writeString(b, "identical content");

        assertEquals(ContentHash.compute(a), ContentHash.compute(b));
    }

    @Test
    void differentFileContentProducesADifferentHash() throws IOException {
        Path a = Files.createTempFile("content-hash-a", ".txt");
        Path b = Files.createTempFile("content-hash-b", ".txt");
        Files.writeString(a, "hello");
        Files.writeString(b, "goodbye");

        assertNotEquals(ContentHash.compute(a), ContentHash.compute(b));
    }
}
