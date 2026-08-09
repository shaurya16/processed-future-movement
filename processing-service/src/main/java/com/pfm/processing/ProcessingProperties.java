package com.pfm.processing;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "processing")
public record ProcessingProperties(String topic) {
}
