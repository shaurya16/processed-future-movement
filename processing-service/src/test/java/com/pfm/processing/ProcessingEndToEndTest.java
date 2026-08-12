package com.pfm.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.ReportKey;
import com.pfm.processing.report.ReportEntry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ProcessingEndToEndTest {

    // Matches the key fields set on transaction() below. This test publishes directly via a
    // raw KafkaProducer (no ingestion-service in the loop), so it must build the Kafka message
    // key the same way KafkaKeyBuilder does in production: ReportKey.from(transaction).encode().
    // A hardcoded "clientInformation|productInformation" literal would no longer decode -- the
    // state store's key is now the full 8-field ReportKey encoding, not that 2-part string.
    private static final ReportKey REPORT_KEY =
            new ReportKey("CL", "4321", "0002", "0001", "SGX", "FU", "NK", LocalDate.of(2010, 9, 10));
    private static final String KEY = REPORT_KEY.encode();

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.2");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.streams.application-id", () -> "processing-service-e2e-test");
    }

    // processing-service deliberately never creates future-transactions itself (see the k8s
    // slice's ordering-bug design) -- in production, ingestion-service's NewTopic bean always
    // creates it first. This test has no ingestion-service context at all (it publishes directly
    // via a raw KafkaProducer), so without this, Kafka Streams' StreamThread can start against a
    // fresh broker with no future-transactions topic yet, hit "Missing source topics", and
    // fatally, permanently die (PENDING_ERROR -> DEAD -> ERROR) with no self-recovery -- the
    // exact failure mode the ordering-bug fix exists to prevent in the real deployment, just
    // reproduced here as flaky test failures instead. Creating the topic before the Spring
    // context (and therefore Kafka Streams) starts removes the race entirely.
    @BeforeAll
    static void createFutureTransactionsTopic() throws Exception {
        Map<String, Object> adminProps = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic("future-transactions", 3, (short) 1)))
                    .all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    @Autowired
    TestRestTemplate restTemplate;

    private KafkaProducer<String, FutureTransaction> producer;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        producer = new KafkaProducer<>(producerProps, new StringSerializer(), new JsonSerializer<>(objectMapper));
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    void reportReflectsDedupedAggregateAcrossARepublishedDuplicate() throws Exception {
        FutureTransaction transaction = transaction(100, 30); // net +70 per occurrence

        publish(KEY, transaction, "e2e-tx-1");
        publish(KEY, transaction, "e2e-tx-1"); // duplicate republish: must not double count
        publish(KEY, transaction, "e2e-tx-2"); // legitimate second trade: must count

        awaitReportEntry(KEY, 140L);
    }

    private void publish(String key, FutureTransaction transaction, String transactionId) throws Exception {
        ProducerRecord<String, FutureTransaction> record = new ProducerRecord<>(
                "future-transactions", null, key, transaction,
                List.of(new RecordHeader("transactionId", transactionId.getBytes(StandardCharsets.UTF_8))));
        producer.send(record).get(10, TimeUnit.SECONDS);
    }

    private void awaitReportEntry(String key, long expectedNetQuantity) {
        // 60s (bumped from 30s) is headroom for Kafka Streams to reach RUNNING on a
        // slower/shared CI runner, matching the budget FullPipelineGoldenTest already uses for
        // the same kind of wait. Not the primary fix for this test's flakiness, though: that was
        // createFutureTransactionsTopic() above, guaranteeing the topic exists before Kafka
        // Streams ever starts -- without it, no timeout here would help, since a stream thread
        // that dies from missing source topics never recovers.
        ReportKey reportKey = ReportKey.decode(key);
        long deadline = System.currentTimeMillis() + 60_000;
        ResponseEntity<ReportEntry[]> lastResponse = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                ResponseEntity<ReportEntry[]> response = restTemplate.getForEntity("/api/v1/report", ReportEntry[].class);
                lastResponse = response;
                if (response.getStatusCode().value() == 200 && response.getBody() != null) {
                    for (ReportEntry entry : response.getBody()) {
                        if (entry.clientInformation().equals(reportKey.clientInformation())
                                && entry.productInformation().equals(reportKey.productInformation())
                                && entry.netQuantity() == expectedNetQuantity) {
                            return;
                        }
                    }
                }
            } catch (org.springframework.web.client.RestClientException e) {
                // Kafka Streams may not be RUNNING yet: ReportController returns 503 with a
                // JSON error object in that case, and Jackson throws trying to deserialize
                // that object as ReportEntry[] before status-code handling ever runs. Treat
                // any such failure as "not ready yet" and keep polling.
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        fail("expected report entry for " + key + " with netQuantity=" + expectedNetQuantity
                + " not observed within 60s; last response body: "
                + (lastResponse == null ? "null" : java.util.Arrays.toString(lastResponse.getBody())));
    }

    private FutureTransaction transaction(long quantityLong, long quantityShort) {
        return new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B',
                quantityLong, quantityShort,
                BigDecimal.valueOf(60, 2), "USD", 'D',
                BigDecimal.valueOf(30, 2), "USD", 'D',
                BigDecimal.ZERO, "JPY", 'D',
                LocalDate.of(2010, 8, 20), "001238", "0", "688032",
                BigDecimal.valueOf(925, 5), "TRDR12", "OPP0001", 'O');
    }
}
