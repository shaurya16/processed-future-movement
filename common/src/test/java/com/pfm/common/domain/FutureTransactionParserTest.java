package com.pfm.common.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FutureTransactionParserTest {

    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    private static final String LINE_13 =
        "315CL  432100030001FCC   FUCME N1    20100910JPY01S 0000000000 0000000003000000000000DUSD000000000015DUSD000000000000DJPY20100819059475      000308000093300000000             O";

    private static final String TRUNCATED_LINE = "315CL";

    private final FutureTransactionParser parser = new FutureTransactionParser();

    @Test
    void parseAllReturnsAllRecordsWhenEveryLineIsValid() {
        ParseResult result = parser.parseAll(List.of(LINE_1, LINE_13));

        assertEquals(2, result.records().size());
        assertTrue(result.errors().isEmpty());
        assertEquals("0002", result.records().get(0).accountNumber());
        assertEquals("0003", result.records().get(1).accountNumber());
    }

    @Test
    void parseAllSkipsBadLineAndCollectsError() {
        ParseResult result = parser.parseAll(List.of(LINE_1, TRUNCATED_LINE));

        assertEquals(1, result.records().size());
        assertEquals("0002", result.records().get(0).accountNumber());
        assertEquals(1, result.errors().size());
        assertEquals(2, result.errors().get(0).lineNumber());
        assertEquals(TRUNCATED_LINE, result.errors().get(0).rawLine());
    }

    @Test
    void parseAllRecoversAfterABadLineAndKeepsParsingSubsequentGoodLines() {
        ParseResult result = parser.parseAll(List.of(LINE_1, TRUNCATED_LINE, LINE_13));

        assertEquals(2, result.records().size());
        assertEquals("0002", result.records().get(0).accountNumber());
        assertEquals("0003", result.records().get(1).accountNumber());
        assertEquals(1, result.errors().size());
        assertEquals(2, result.errors().get(0).lineNumber());
    }

    @Test
    void parseAllReturnsOnlyErrorsWhenEveryLineIsBad() {
        ParseResult result = parser.parseAll(List.of(TRUNCATED_LINE, "x"));

        assertTrue(result.records().isEmpty());
        assertEquals(2, result.errors().size());
        assertEquals(1, result.errors().get(0).lineNumber());
        assertEquals(2, result.errors().get(1).lineNumber());
    }

    @Test
    void strictParseThrowsOnBadLine() {
        org.junit.jupiter.api.Assertions.assertThrows(
            com.pfm.common.fixedwidth.FixedWidthParseException.class,
            () -> parser.parse(TRUNCATED_LINE, 1));
    }
}
