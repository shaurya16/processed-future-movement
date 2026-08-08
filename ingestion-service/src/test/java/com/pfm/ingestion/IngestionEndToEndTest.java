package com.pfm.ingestion;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class IngestionEndToEndTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.2");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("ingestion.file-path", IngestionEndToEndTest::sampleFilePath);
    }

    @Autowired
    TestRestTemplate restTemplate;

    private static String sampleFilePath() {
        try {
            return Path.of(IngestionEndToEndTest.class.getClassLoader().getResource("Input.txt").toURI())
                    .toAbsolutePath()
                    .toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void ingestingSampleFilePublishesAllRecordsAndIsIdempotentUntilForced() {
        IngestionResult first = postIngest("");
        assertEquals(717, first.totalLines());
        assertEquals(717, first.published());
        assertEquals(0, first.skipped());
        assertFalse(first.cached());

        IngestionResult second = postIngest("");
        assertTrue(second.cached());
        assertEquals(first.published(), second.published());

        IngestionResult forced = postIngest("?force=true");
        assertFalse(forced.cached());
        assertEquals(717, forced.published());

        List<ConsumerRecord<String, String>> records = consumeAll(1434);
        assertEquals(1434, records.size());
        assertTrue(records.stream().allMatch(r -> r.key() != null && r.key().contains("|")));
    }

    private IngestionResult postIngest(String querySuffix) {
        ResponseEntity<IngestionResult> response =
                restTemplate.postForEntity("/api/ingest" + querySuffix, null, IngestionResult.class);
        assertEquals(200, response.getStatusCode().value());
        IngestionResult body = response.getBody();
        assertNotNull(body);
        return body;
    }

    private List<ConsumerRecord<String, String>> consumeAll(int expectedCount) {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> collected = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(List.of("future-transactions"));
            long deadline = System.currentTimeMillis() + 20_000;
            while (collected.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(collected::add);
            }
        }
        return collected;
    }
}
