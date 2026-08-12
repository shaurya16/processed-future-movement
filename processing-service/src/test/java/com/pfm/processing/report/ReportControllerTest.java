package com.pfm.processing.report;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

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
                new ReportEntry("CL432100020001", "SGXFUNK20100910", 46L),
                new ReportEntry("CL432100030001", "CMEFUN120100910", -79L)));

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
                new ReportEntry("CL123400020001", "SGXFUNK20100910", -52L),
                new ReportEntry("CL123400030001", "CMEFUN120100910", 285L),
                new ReportEntry("CL123400030001", "CMEFUNK.20100910", -215L),
                new ReportEntry("CL432100020001", "SGXFUNK20100910", 46L),
                new ReportEntry("CL432100030001", "CMEFUN120100910", -79L)));

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
}
