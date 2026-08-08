package com.pfm.ingestion;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class IngestionController {

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
        return Map.of("error", e.getMessage());
    }
}
