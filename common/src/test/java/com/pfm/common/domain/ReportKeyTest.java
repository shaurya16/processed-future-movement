package com.pfm.common.domain;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReportKeyTest {

    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    private final FutureTransactionParser parser = new FutureTransactionParser();

    private ReportKey keyOfLine1() {
        return ReportKey.from(parser.parse(LINE_1, 1));
    }

    @Test
    void fromExtractsTheEightGroupingFields() {
        ReportKey key = keyOfLine1();

        assertEquals("CL", key.clientType());
        assertEquals("4321", key.clientNumber());
        assertEquals("0002", key.accountNumber());
        assertEquals("0001", key.subaccountNumber());
        assertEquals("SGX", key.exchangeCode());
        assertEquals("FU", key.productGroupCode());
        assertEquals("NK", key.symbol());
        assertEquals(LocalDate.of(2010, 9, 10), key.expirationDate());
    }

    @Test
    void encodeJoinsAllEightFieldsWithPipes() {
        assertEquals("CL|4321|0002|0001|SGX|FU|NK|20100910", keyOfLine1().encode());
    }

    @Test
    void decodeReversesEncode() {
        ReportKey original = keyOfLine1();

        assertEquals(original, ReportKey.decode(original.encode()));
    }

    @Test
    void derivedInformationFieldsMatchTheReportSpec() {
        ReportKey key = keyOfLine1();

        // These two are the CSV column values; they must be byte-identical to
        // the concatenation rule in docs/file-spec.md.
        assertEquals("CL432100020001", key.clientInformation());
        assertEquals("SGXFUNK20100910", key.productInformation());
    }

    @Test
    void derivedProductInformationHandlesAVariableLengthSymbol() {
        // 'NK.' is 3 chars where line 1's symbol is 2 — the ambiguity that makes
        // client-side splitting impossible, and why the key carries all 8 fields.
        ReportKey key = new ReportKey("CL", "1234", "0003", "0001",
                "CME", "FU", "NK.", LocalDate.of(2010, 9, 10));

        assertEquals("CMEFUNK.20100910", key.productInformation());
        assertEquals("CL|1234|0003|0001|CME|FU|NK.|20100910", key.encode());
    }

    @Test
    void decodeRejectsAKeyThatIsNotEightParts() {
        // The pre-change 2-part format must fail loudly rather than yield blanks.
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ReportKey.decode("CL432100020001|SGXFUNK20100910"));

        assertEquals("Expected 8 pipe-delimited parts in report key, got 2: "
                + "CL432100020001|SGXFUNK20100910", e.getMessage());
    }

    @Test
    void decodePreservesTrailingEmptyFields() {
        // split() must use limit -1 or a blank trailing symbol silently drops a part.
        ReportKey key = ReportKey.decode("CL|4321|0002|0001|SGX|FU||20100910");

        assertEquals("", key.symbol());
        assertEquals(LocalDate.of(2010, 9, 10), key.expirationDate());
    }
}
