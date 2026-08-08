package com.pfm.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
class IngestionServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
