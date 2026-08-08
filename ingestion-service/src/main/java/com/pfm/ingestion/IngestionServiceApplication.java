package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransactionParser;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
@ConfigurationPropertiesScan
public class IngestionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngestionServiceApplication.class, args);
    }

    @Bean
    public FutureTransactionParser futureTransactionParser() {
        return new FutureTransactionParser();
    }
}
