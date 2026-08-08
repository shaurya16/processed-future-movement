package com.pfm.ingestion;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingestion")
public record IngestionProperties(String filePath, String topic) {
}
