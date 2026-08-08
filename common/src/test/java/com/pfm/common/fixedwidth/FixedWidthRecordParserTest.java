package com.pfm.common.fixedwidth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FixedWidthRecordParserTest {

    // Real line 1 from sample-data/Input.txt: client 4321, SGX/NK buy record.
    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    record SampleRecord(
        @FixedWidthField(start = 1, length = 3) String recordCode,
        @FixedWidthField(start = 4, length = 4) String clientType,
        @FixedWidthField(start = 8, length = 4) String clientNumber
    ) {}

    private final FixedWidthRecordParser parser = new FixedWidthRecordParser();

    @Test
    void extractsAnnotatedFieldsFromRealLine() {
        SampleRecord result = parser.parse(LINE_1, 1, SampleRecord.class);

        assertEquals("315", result.recordCode());
        assertEquals("CL", result.clientType());
        assertEquals("4321", result.clientNumber());
    }

    @Test
    void throwsWhenLineTooShortForField() {
        String truncated = "315CL";

        FixedWidthParseException exception = assertThrows(FixedWidthParseException.class,
            () -> parser.parse(truncated, 7, SampleRecord.class));

        assertEquals(7, exception.lineNumber());
        assertEquals(truncated, exception.rawLine());
    }
}
