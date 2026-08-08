package com.pfm.common.domain;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GoldenSampleFileTest {

    @Test
    void parsesEntireSampleFileWithNoErrors() throws IOException {
        List<String> lines = readSampleLines();

        ParseResult result = new FutureTransactionParser().parseAll(lines);

        assertEquals(717, result.records().size());
        assertTrue(result.errors().isEmpty(), "unexpected parse errors: " + result.errors());
    }

    private List<String> readSampleLines() throws IOException {
        List<String> lines = new ArrayList<>();
        try (InputStream in = getClass().getResourceAsStream("/Input.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }
}
