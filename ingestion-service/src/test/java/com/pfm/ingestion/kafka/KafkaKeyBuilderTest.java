package com.pfm.ingestion.kafka;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.ReportKey;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaKeyBuilderTest {

    @Test
    void buildsKeyCarryingAllEightGroupingFields() {
        FutureTransaction transaction = sampleTransaction();

        // 8 parts, not the previous 2 — the sub-fields must survive the round trip
        // so processing-service can expose them as columns.
        assertEquals("CL|4321|0002|0001|SGX|FU|NK|20100910", KafkaKeyBuilder.buildKey(transaction));
    }

    @Test
    void keyDecodesBackToTheDerivedReportColumns() {
        FutureTransaction transaction = sampleTransaction();

        ReportKey decoded = ReportKey.decode(KafkaKeyBuilder.buildKey(transaction));

        assertEquals("CL432100020001", decoded.clientInformation());
        assertEquals("SGXFUNK20100910", decoded.productInformation());
    }

    private static FutureTransaction sampleTransaction() {
        return new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B', 1L, 0L,
                new BigDecimal("-0.60"), "USD", 'D',
                new BigDecimal("-0.30"), "USD", 'D',
                new BigDecimal("0.00"), "JPY", 'D',
                LocalDate.of(2010, 8, 20), "001238", "0", "688032",
                new BigDecimal("9250.0000000"), "", "", 'O'
        );
    }
}
