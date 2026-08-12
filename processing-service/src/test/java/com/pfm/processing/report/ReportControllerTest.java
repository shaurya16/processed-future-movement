package com.pfm.processing.report;

import com.pfm.common.domain.NetPosition;
import com.pfm.common.domain.ReportKey;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ReportController.class)
class ReportControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    ReportService reportService;

    @Test
    void getReportReturnsJsonWithSpecFieldNames() throws Exception {
        when(reportService.currentReport()).thenReturn(List.of(
                entry("CL", "4321", "0002", "0001", "SGX", "FU", "NK", 46L),
                entry("CL", "4321", "0003", "0001", "CME", "FU", "N1", -79L)));

        mockMvc.perform(get("/api/v1/report"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].Client_Information").value("CL432100020001"))
                .andExpect(jsonPath("$[0].Product_Information").value("SGXFUNK20100910"))
                .andExpect(jsonPath("$[0].Total_Transaction_Amount").value(46))
                .andExpect(jsonPath("$[1].Client_Information").value("CL432100030001"))
                .andExpect(jsonPath("$[1].Total_Transaction_Amount").value(-79));
    }

    @Test
    void getReportReturnsEmptyArrayWhenNothingAggregatedYet() throws Exception {
        when(reportService.currentReport()).thenReturn(List.of());

        mockMvc.perform(get("/api/v1/report"))
                .andExpect(status().isOk())
                .andExpect(content().json("[]"));
    }

    @Test
    void getReportCsvReturnsExactHeaderAndRows() throws Exception {
        when(reportService.currentReport()).thenReturn(List.of(
                entry("CL", "1234", "0002", "0001", "SGX", "FU", "NK", -52L),
                entry("CL", "1234", "0003", "0001", "CME", "FU", "N1", 285L),
                entry("CL", "1234", "0003", "0001", "CME", "FU", "NK.", -215L),
                entry("CL", "4321", "0002", "0001", "SGX", "FU", "NK", 46L),
                entry("CL", "4321", "0003", "0001", "CME", "FU", "N1", -79L)));

        String expectedCsv = "Client_Information,Product_Information,Total_Transaction_Amount\n"
                + "CL123400020001,SGXFUNK20100910,-52\n"
                + "CL123400030001,CMEFUN120100910,285\n"
                + "CL123400030001,CMEFUNK.20100910,-215\n"
                + "CL432100020001,SGXFUNK20100910,46\n"
                + "CL432100030001,CMEFUN120100910,-79\n";

        mockMvc.perform(get("/api/v1/report/csv"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("text/csv"))
                .andExpect(content().string(expectedCsv));
    }

    @Test
    void getReportReturns503WhenStoreIsNotReady() throws Exception {
        when(reportService.currentReport()).thenThrow(new StoreNotReadyException());

        mockMvc.perform(get("/api/v1/report"))
                .andExpect(status().isServiceUnavailable());
    }

    private static ReportEntry entry(String clientType, String clientNumber, String accountNumber,
            String subaccountNumber, String exchangeCode, String productGroupCode, String symbol,
            long netQuantity) {
        return ReportEntry.of(
                new ReportKey(clientType, clientNumber, accountNumber, subaccountNumber,
                        exchangeCode, productGroupCode, symbol, LocalDate.of(2010, 9, 10)),
                new NetPosition(netQuantity, Math.max(netQuantity, 0), Math.max(-netQuantity, 0), 1,
                        LocalDate.of(2010, 8, 20), LocalDate.of(2010, 8, 20),
                        Instant.parse("2026-08-12T14:31:52Z"), Map.of()));
    }
}
