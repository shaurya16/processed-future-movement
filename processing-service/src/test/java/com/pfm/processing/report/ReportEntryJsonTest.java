package com.pfm.processing.report;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pfm.common.domain.NetPosition;
import com.pfm.common.domain.ReportKey;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportEntryJsonTest {

    @Test
    void serializesWithFrozenLegacyNamesAndIsoTemporalFields() {
        ReportEntry entry = ReportEntry.of(
                new ReportKey("CL", "1234", "0003", "0001", "CME", "FU", "NK.",
                        LocalDate.of(2010, 9, 10)),
                new NetPosition(-215L, 285L, 500L, 12,
                        LocalDate.of(2010, 8, 19), LocalDate.of(2010, 8, 20),
                        Instant.parse("2026-08-12T14:31:52Z"),
                        Map.of("USD", new BigDecimal("-0.90"))));

        // Use Boot's own Jackson configuration so this test reflects what the
        // controller actually emits, not a hand-rolled ObjectMapper.
        new ApplicationContextRunner()
                .withConfiguration(org.springframework.boot.autoconfigure.AutoConfigurations
                        .of(JacksonAutoConfiguration.class))
                .run(context -> {
                    String json = context.getBean(ObjectMapper.class).writeValueAsString(entry);

                    // Frozen names — any consumer of the old contract still works.
                    assertTrue(json.contains("\"Client_Information\":\"CL123400030001\""), json);
                    assertTrue(json.contains("\"Product_Information\":\"CMEFUNK.20100910\""), json);
                    assertTrue(json.contains("\"Total_Transaction_Amount\":-215"), json);
                    // Decomposed dimensions the UI filters on.
                    assertTrue(json.contains("\"clientNumber\":\"1234\""), json);
                    assertTrue(json.contains("\"accountNumber\":\"0003\""), json);
                    assertTrue(json.contains("\"symbol\":\"NK.\""), json);
                    // Dates as ISO strings, not numeric timestamps.
                    assertTrue(json.contains("\"expirationDate\":\"2010-09-10\""), json);
                    assertTrue(json.contains("\"lastTransactionDate\":\"2010-08-20\""), json);
                    // Measures.
                    assertTrue(json.contains("\"grossLong\":285"), json);
                    assertTrue(json.contains("\"grossShort\":500"), json);
                    assertTrue(json.contains("\"tradeCount\":12"), json);
                    assertTrue(json.contains("\"feesByCurrency\":{\"USD\":-0.90}"), json);
                });
    }
}
