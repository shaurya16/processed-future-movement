package com.pfm.processing;

import com.pfm.ingestion.IngestionResult;
import com.pfm.ingestion.IngestionServiceApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
class FullPipelineGoldenTest {

    /**
     * Read from the committed fixture rather than inlined, so this test fails if the
     * pipeline's output and sample-output/Output.csv ever disagree. The copy in
     * src/test/resources must stay in sync with sample-output/Output.csv — the same
     * convention Input.txt already follows.
     */
    private static String expectedCsv() throws IOException {
        try (InputStream in = FullPipelineGoldenTest.class.getResourceAsStream("/Output.csv")) {
            if (in == null) {
                throw new IllegalStateException("Missing test fixture /Output.csv on the classpath");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.2");

    @TempDir
    static Path streamsStateDir;

    private ConfigurableApplicationContext ingestionContext;
    private ConfigurableApplicationContext processingContext;

    @AfterEach
    void tearDown() {
        if (processingContext != null) {
            processingContext.close();
        }
        if (ingestionContext != null) {
            ingestionContext.close();
        }
        System.clearProperty("spring.config.name");
    }

    @Test
    void fullPipelineProducesTheExpectedDailySummary() throws URISyntaxException, IOException {
        // ingestion-service and processing-service both ship an application.yml at the
        // identical classpath location (classpath:/application.yml). Booting both as real
        // apps in this one JVM puts ingestion-service's jar (a test-scope dependency) and
        // processing-service's own target/classes on the same classpath at once; Spring's
        // classpath resolution can only see one application.yml, and Surefire's classpath
        // ordering means processing-service's own copy wins for BOTH contexts -- silently
        // misconfiguring ingestionContext with the wrong port/app name/topic. Suppressing
        // default config-file lookup entirely (a System property, seen by Spring Boot's
        // config-data loading before either context boots) and supplying every needed
        // property explicitly via .properties(...) sidesteps this for both contexts,
        // deterministically, without depending on classpath ordering at all.
        System.setProperty("spring.config.name", "full-pipeline-golden-test-disable-default-config");

        String sampleFilePath = Path.of(getClass().getClassLoader().getResource("Input.txt").toURI())
                .toAbsolutePath()
                .toString();

        ingestionContext = new SpringApplicationBuilder(IngestionServiceApplication.class)
                .properties(
                        "server.port=18081",
                        "spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "ingestion.file-path=" + sampleFilePath,
                        "ingestion.topic=future-transactions")
                .run();

        processingContext = new SpringApplicationBuilder(ProcessingServiceApplication.class)
                .properties(
                        "server.port=18082",
                        "spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
                        "spring.kafka.streams.application-id=processing-service-golden-test",
                        "spring.kafka.streams.state-dir=" + streamsStateDir.toAbsolutePath(),
                        "processing.topic=future-transactions")
                .run();

        RestTemplate rest = new RestTemplate();
        IngestionResult ingestResult = rest.postForObject(
                "http://localhost:18081/api/v1/ingest", null, IngestionResult.class);
        assertEquals(717, ingestResult.published());

        String expected = expectedCsv();
        String csv = awaitFullReportCsv(rest, expected);
        assertEquals(expected, csv,
                "CSV output must stay byte-identical to sample-output/Output.csv");
    }

    private String awaitFullReportCsv(RestTemplate rest, String expectedCsv) {
        // Polling for a 6-line response (header + 5 rows) is not a sufficient "done" signal:
        // Kafka Streams materializes the KTable incrementally as it consumes the topic, so all
        // 5 distinct keys can appear in the store well before every one of the 717 records has
        // actually been folded into their aggregates. On a fast machine that race window is too
        // narrow to hit; on a slower/shared CI runner it isn't, producing a report snapshot with
        // the right keys but partially-aggregated values. Polling for the exact expected content
        // instead makes "done" mean "the aggregation has actually converged," not just "the
        // right rows exist yet."
        long deadline = System.currentTimeMillis() + 60_000;
        String lastBody = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                lastBody = rest.getForObject("http://localhost:18082/api/v1/report/csv", String.class);
                if (expectedCsv.equals(lastBody)) {
                    return lastBody;
                }
            } catch (RestClientException ignored) {
                // processing-service's Kafka Streams app may not have finished starting yet.
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        fail("processing-service did not produce the expected report within 60s; last body: " + lastBody);
        return null;
    }
}
