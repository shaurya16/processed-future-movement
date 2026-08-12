package com.pfm.ingestion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class IngestionController {

    private static final Logger log = LoggerFactory.getLogger(IngestionController.class);

    private final IngestionService ingestionService;

    public IngestionController(IngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @PostMapping("/ingest")
    public IngestionResult ingest(@RequestParam(name = "force", defaultValue = "false") boolean force) {
        return ingestionService.ingest(force);
    }

    @ExceptionHandler(KafkaPublishException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public Map<String, String> handleKafkaPublishFailure(KafkaPublishException e) {
        return Map.of("error", e.getMessage());
    }

    @ExceptionHandler(IngestionFileNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public Map<String, String> handleFileNotFound(IngestionFileNotFoundException e) {
        // Log the full detail (including absolute path) server-side only; the HTTP
        // response must not leak the host/container filesystem layout to callers.
        log.warn(e.getMessage());
        return Map.of("error", "Ingestion file not found");
    }
}
