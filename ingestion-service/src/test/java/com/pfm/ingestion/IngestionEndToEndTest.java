package com.pfm.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

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

        // (a) Wire-contract check on the key: 8 pipe-delimited fields
        // clientType|clientNumber|accountNumber|subaccountNumber|exchangeCode|productGroupCode|symbol|yyyyMMdd
        // The expiration date must be raw CCYYMMDD (e.g. "20100910"), not dashed ISO format (e.g. "2010-09-10").
        // Real symbols can contain punctuation (e.g. "NK."), so we don't over-constrain the charset.
        Pattern datePattern = Pattern.compile("^\\d{8}$");
        assertTrue(records.stream().allMatch(r -> {
            if (r.key() == null) {
                return false;
            }
            String[] parts = r.key().split("\\|", -1);
            return parts.length == 8 && !parts[0].isEmpty() && !parts[7].isEmpty() && datePattern.matcher(parts[7]).matches();
        }), "every record key must have 8 pipe-delimited parts: "
                + "clientType|clientNumber|accountNumber|subaccountNumber|exchangeCode|productGroupCode|symbol|yyyyMMdd, "
                + "e.g. CL|4321|0002|0001|SGX|FU|NK|20100910");

        // (b) Wire-contract check on the value: deserialize the JSON payload back into a
        // FutureTransaction using a JavaTimeModule-aware ObjectMapper (mirroring what a real
        // consumer would need) and confirm it round-trips a recognizable record, including
        // that LocalDate fields serialized as ISO-8601 strings, not timestamp arrays. This is
        // what would have caught finding #1 (a bare ObjectMapper defaults to
        // WRITE_DATES_AS_TIMESTAMPS, which serializes LocalDate as e.g. [2010,9,10]).
        ObjectMapper valueMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        ConsumerRecord<String, String> sample = records.get(0);
        assertTrue(sample.value().contains("\"expirationDate\":\"20"),
                "expected expirationDate to be serialized as an ISO-8601 string, got: " + sample.value());
        FutureTransaction transaction;
        try {
            transaction = valueMapper.readValue(sample.value(), FutureTransaction.class);
        } catch (Exception e) {
            throw new AssertionError("failed to deserialize Kafka value as FutureTransaction: " + sample.value(), e);
        }
        assertNotNull(transaction.expirationDate());
        assertTrue(transaction.expirationDate().isAfter(LocalDate.of(1900, 1, 1)),
                "expirationDate should have round-tripped to a real LocalDate, got: " + transaction.expirationDate());
        assertNotNull(transaction.symbol());

        // (c) Wire-contract check on headers: every record carries a transactionId header
        // that looks like a SHA-256 hex digest (64 lowercase hex chars) — the mechanism
        // that lets processing-service dedupe a retried/forced re-publish of this exact
        // file without double-counting. Exact values are covered precisely at the unit
        // level (IngestionServiceTest, ContentHashTest, TransactionIdBuilderTest); this is
        // a structural check that the header actually reaches a real broker.
        Pattern sha256HexPattern = Pattern.compile("^[0-9a-f]{64}$");
        assertTrue(records.stream().allMatch(r -> {
            Header header = r.headers().lastHeader("transactionId");
            if (header == null || header.value() == null) {
                return false;
            }
            String value = new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
            return sha256HexPattern.matcher(value).matches();
        }), "every record must carry a transactionId header that is a 64-char lowercase hex SHA-256 digest");
    }

    private IngestionResult postIngest(String querySuffix) {
        ResponseEntity<IngestionResult> response =
                restTemplate.postForEntity("/api/v1/ingest" + querySuffix, null, IngestionResult.class);
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
