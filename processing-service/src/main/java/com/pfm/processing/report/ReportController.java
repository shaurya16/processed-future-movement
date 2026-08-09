package com.pfm.processing.report;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class ReportController {

    private final ReportService reportService;

    public ReportController(ReportService reportService) {
        this.reportService = reportService;
    }

    @GetMapping("/api/report")
    public List<ReportEntry> report() {
        return reportService.currentReport();
    }

    @GetMapping(value = "/api/report/csv", produces = "text/csv")
    public ResponseEntity<String> reportCsv() {
        List<ReportEntry> entries = reportService.currentReport();
        StringBuilder csv = new StringBuilder("Client_Information,Product_Information,Total_Transaction_Amount\n");
        for (ReportEntry entry : entries) {
            csv.append(entry.clientInformation())
                    .append(',')
                    .append(entry.productInformation())
                    .append(',')
                    .append(entry.netQuantity())
                    .append('\n');
        }
        return ResponseEntity.ok()
                .contentType(MediaType.valueOf("text/csv"))
                .header("Content-Disposition", "attachment; filename=\"Output.csv\"")
                .body(csv.toString());
    }

    @ExceptionHandler(StoreNotReadyException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> handleStoreNotReady(StoreNotReadyException e) {
        return Map.of("error", e.getMessage());
    }
}
