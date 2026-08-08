# Ingestion Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `ingestion-service` — a Spring Boot module exposing `POST /api/ingest`, which reads the configured fixed-width file via `common`'s `FutureTransactionParser`, publishes one JSON Kafka event per parsed record to `future-transactions` (keyed by client+product), and is idempotent per file version.

**Architecture:** `IngestionController` (REST) → `IngestionService` (orchestration: fingerprint, dedup via `IngestionRegistry`, parse via `common`, publish via `KafkaTemplate`) → Kafka. `IngestionRegistry` is an in-memory `ConcurrentHashMap`-backed cache keyed by file fingerprint (path+size+mtime), using `computeIfAbsent` so concurrent/duplicate calls for the same file version compute at most once.

**Tech Stack:** Java 21, Spring Boot 3.5.4 (own `spring-boot-starter-parent`, sibling to the plain-Java root aggregator POM used by `common`), Spring Kafka 3.3.8 (Boot-managed), Testcontainers 1.21.3 (Boot-managed) for integration tests, JUnit 5 + Mockito (via `spring-boot-starter-test`).

## Global Constraints

- Java 21 (`java.version=21` in `ingestion-service/pom.xml`, matching root's `maven.compiler.release=21`).
- Package root: `com.pfm.ingestion`.
- `ingestion-service`'s Maven `<parent>` is `org.springframework.boot:spring-boot-starter-parent:3.5.4` — **not** the root `com.pfm:processed-future-movement` POM (that stays `common`'s parent). Root `pom.xml` still lists `ingestion-service` under `<modules>` for the reactor build; Maven aggregation doesn't require a shared parent. This is the standard pattern for a plain-Java root + Spring Boot child module and is called out explicitly here so it doesn't read as a mistake during review.
- All Spring/Kafka/Testcontainers dependency versions come from `spring-boot-starter-parent`'s managed BOM (`spring-boot-dependencies:3.5.4`) — no explicit `<version>` on `spring-boot-starter-web`, `spring-kafka`, `org.testcontainers:kafka`, `org.testcontainers:junit-jupiter`, or `spring-boot-starter-test`.
- File path (`ingestion.file-path`) and topic (`ingestion.topic`) are Spring `@ConfigurationProperties`, resolved from `application.yml` with `${ENV_VAR:default}` placeholders — never hardcoded in Java source, per design decision #2.
- Design reference: [docs/superpowers/specs/2026-08-09-ingestion-service-design.md](../specs/2026-08-09-ingestion-service-design.md) — every task below implements one or more of its 10 numbered decisions.
- One commit per task, after that task's tests pass.

---

### Task 1: Maven module scaffold + application bootstrap

**Files:**
- Modify: `pom.xml` (repo root) — add `<module>ingestion-service</module>`
- Create: `ingestion-service/pom.xml`
- Create: `ingestion-service/src/main/resources/application.yml`
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionServiceApplication.java`
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionProperties.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceApplicationTests.java`

**Interfaces:**
- Consumes: `com.pfm:common:0.1.0-SNAPSHOT` (existing, already built and tested).
- Produces: a bootable Spring Boot application; `IngestionProperties` record with `filePath()`/`topic()` accessors, bound from `ingestion.*` config, consumed by later tasks.

- [ ] **Step 1: Add `ingestion-service` to the root reactor**

Edit `pom.xml`, inside `<modules>`:

```xml
  <modules>
    <module>common</module>
    <module>ingestion-service</module>
  </modules>
```

- [ ] **Step 2: Create `ingestion-service/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.5.4</version>
    <relativePath/>
  </parent>

  <groupId>com.pfm</groupId>
  <artifactId>ingestion-service</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <properties>
    <java.version>21</java.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>com.pfm</groupId>
      <artifactId>common</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>

    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.kafka</groupId>
      <artifactId>spring-kafka</artifactId>
    </dependency>

    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>kafka</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.testcontainers</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.testcontainers</groupId>
        <artifactId>testcontainers-bom</artifactId>
        <version>1.21.3</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

Note: `spring-boot-dependencies` (the parent's managed BOM) already imports `testcontainers-bom:1.21.3` internally, so the explicit `<dependencyManagement>` import here is redundant in principle — it's included anyway to make the Testcontainers version an explicit, greppable fact in this module's own POM rather than an inherited implicit one. If `mvn -pl ingestion-service -am dependency:tree` later shows a version conflict, delete this block and rely on the parent's management instead.

- [ ] **Step 3: Create `application.yml`**

`ingestion-service/src/main/resources/application.yml`:

```yaml
server:
  port: 8081

spring:
  application:
    name: ingestion-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

ingestion:
  file-path: ${INGESTION_FILE_PATH:sample-data/Input.txt}
  topic: future-transactions
```

- [ ] **Step 4: Create `IngestionProperties`**

`ingestion-service/src/main/java/com/pfm/ingestion/IngestionProperties.java`:

```java
package com.pfm.ingestion;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingestion")
public record IngestionProperties(String filePath, String topic) {
}
```

- [ ] **Step 5: Create the application class**

`ingestion-service/src/main/java/com/pfm/ingestion/IngestionServiceApplication.java`:

```java
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
```

- [ ] **Step 6: Write the failing context-loads test**

`ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceApplicationTests.java`:

```java
package com.pfm.ingestion;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
class IngestionServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
```

Note on `spring.kafka.admin.auto-create=false`: without it, Spring's `KafkaAdmin` tries to
provision any `NewTopic` beans against a live broker on every context refresh (including
this test's), which would hang/retry for several seconds against `localhost:9092` if
nothing is listening there. This override only affects this test; the real
`application.yml` leaves auto-create at its default (`true`), which is what makes
decision #8's `NewTopic` bean actually provision the topic against a real broker in
Task 8's Testcontainers test and in normal `docker compose` usage. `NewTopic` doesn't
exist as a bean yet at this point in the plan, so this override is a no-op today — it's
here now so Task 5 doesn't need to touch this file again.

- [ ] **Step 7: Run it**

Run: `mvn -q -pl ingestion-service -am test`
Expected: `BUILD SUCCESS` — the context loads with no live Kafka broker required (Boot's Kafka autoconfiguration only builds config-holding beans; it doesn't connect eagerly).

- [ ] **Step 8: Commit**

```bash
git add pom.xml ingestion-service/pom.xml ingestion-service/src/main/resources/application.yml \
        ingestion-service/src/main/java/com/pfm/ingestion/IngestionServiceApplication.java \
        ingestion-service/src/main/java/com/pfm/ingestion/IngestionProperties.java \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceApplicationTests.java
git commit -m "feat(ingestion-service): scaffold Spring Boot module"
```

---

### Task 2: `FileFingerprint`

**Files:**
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/FileFingerprint.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/FileFingerprintTest.java`

**Interfaces:**
- Consumes: nothing new.
- Produces: `FileFingerprint.compute(Path path)` → `String`, used by `IngestionService` (Task 6) as the `IngestionRegistry` cache key.

- [ ] **Step 1: Write the failing tests**

```java
package com.pfm.ingestion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class FileFingerprintTest {

    @Test
    void sameFileProducesSameFingerprintAcrossCalls(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("input.txt");
        Files.writeString(file, "hello");

        String first = FileFingerprint.compute(file);
        String second = FileFingerprint.compute(file);

        assertEquals(first, second);
    }

    @Test
    void differentSizeProducesDifferentFingerprint(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("input.txt");
        Files.writeString(file, "hello");
        String before = FileFingerprint.compute(file);

        Files.writeString(file, "hello world, now longer");
        String after = FileFingerprint.compute(file);

        assertNotEquals(before, after);
    }

    @Test
    void differentLastModifiedProducesDifferentFingerprint(@TempDir Path tempDir) throws IOException {
        Path file = tempDir.resolve("input.txt");
        Files.writeString(file, "hello");
        String before = FileFingerprint.compute(file);

        Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis() + 60_000));
        String after = FileFingerprint.compute(file);

        assertNotEquals(before, after);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl ingestion-service -am test -Dtest=FileFingerprintTest`
Expected: FAIL — `FileFingerprint` does not exist yet (compile error).

- [ ] **Step 3: Implement `FileFingerprint`**

```java
package com.pfm.ingestion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class FileFingerprint {

    private FileFingerprint() {
    }

    public static String compute(Path path) throws IOException {
        long size = Files.size(path);
        long lastModifiedMillis = Files.getLastModifiedTime(path).toMillis();
        return path.toAbsolutePath() + "|" + size + "|" + lastModifiedMillis;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl ingestion-service -am test -Dtest=FileFingerprintTest`
Expected: `BUILD SUCCESS`, 3/3 passing.

- [ ] **Step 5: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/FileFingerprint.java \
        ingestion-service/src/test/java/com/pfm/ingestion/FileFingerprintTest.java
git commit -m "feat(ingestion-service): add FileFingerprint for dedup cache keys"
```

---

### Task 3: `KafkaKeyBuilder`

**Files:**
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaKeyBuilder.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaKeyBuilderTest.java`

**Interfaces:**
- Consumes: `com.pfm.common.domain.FutureTransaction` (existing).
- Produces: `KafkaKeyBuilder.buildKey(FutureTransaction)` → `String`, used by `IngestionService` (Task 6) as the Kafka producer record key.

The expected key value below (`CL432100020001|SGXFUNK2010-09-10`) was computed by running the real `FutureTransactionParser` from `common` over the first record of `sample-data/Input.txt` — it is not a hand-derived guess.

- [ ] **Step 1: Write the failing test**

```java
package com.pfm.ingestion.kafka;

import com.pfm.common.domain.FutureTransaction;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaKeyBuilderTest {

    @Test
    void buildsCompositeKeyFromClientAndProductInformation() {
        FutureTransaction transaction = new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B', 1L, 0L,
                new BigDecimal("-0.60"), "USD", 'D',
                new BigDecimal("-0.30"), "USD", 'D',
                new BigDecimal("0.00"), "JPY", 'D',
                LocalDate.of(2010, 8, 20), "001238", "0", "688032",
                new BigDecimal("9250.0000000"), "", "", 'O'
        );

        assertEquals("CL432100020001|SGXFUNK2010-09-10", KafkaKeyBuilder.buildKey(transaction));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl ingestion-service -am test -Dtest=KafkaKeyBuilderTest`
Expected: FAIL — `KafkaKeyBuilder` does not exist yet.

- [ ] **Step 3: Implement `KafkaKeyBuilder`**

```java
package com.pfm.ingestion.kafka;

import com.pfm.common.domain.FutureTransaction;

public final class KafkaKeyBuilder {

    private KafkaKeyBuilder() {
    }

    public static String buildKey(FutureTransaction transaction) {
        String clientInformation = transaction.clientType() + transaction.clientNumber()
                + transaction.accountNumber() + transaction.subaccountNumber();
        String productInformation = transaction.exchangeCode() + transaction.productGroupCode()
                + transaction.symbol() + transaction.expirationDate();
        return clientInformation + "|" + productInformation;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -pl ingestion-service -am test -Dtest=KafkaKeyBuilderTest`
Expected: `BUILD SUCCESS`, 1/1 passing.

- [ ] **Step 5: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaKeyBuilder.java \
        ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaKeyBuilderTest.java
git commit -m "feat(ingestion-service): add KafkaKeyBuilder for client+product composite keys"
```

---

### Task 4: `IngestionResult` and `IngestionRegistry`

**Files:**
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionResult.java`
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionRegistry.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionRegistryTest.java`

**Interfaces:**
- Consumes: `com.pfm.common.fixedwidth.ParseError` (existing).
- Produces:
  - `IngestionResult(String fingerprint, int totalLines, int published, int skipped, List<ParseError> errors, boolean cached)` — the REST response shape, and `IngestionResult.withCached(boolean)` → `IngestionResult`.
  - `IngestionRegistry.CacheOutcome(IngestionResult result, boolean cached)`.
  - `IngestionRegistry.getOrCompute(String fingerprint, Supplier<IngestionResult> computation)` → `CacheOutcome`.
  - `IngestionRegistry.forceCompute(String fingerprint, Supplier<IngestionResult> computation)` → `IngestionResult`.
  Both consumed by `IngestionService` (Task 6).

- [ ] **Step 1: Create `IngestionResult`**

```java
package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;

import java.util.List;

public record IngestionResult(
        String fingerprint,
        int totalLines,
        int published,
        int skipped,
        List<ParseError> errors,
        boolean cached
) {
    public IngestionResult withCached(boolean cached) {
        return new IngestionResult(fingerprint, totalLines, published, skipped, errors, cached);
    }
}
```

- [ ] **Step 2: Write the failing `IngestionRegistry` tests**

```java
package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class IngestionRegistryTest {

    @Test
    void firstCallComputesAndSecondCallReturnsCachedResultWithoutRecomputing() {
        IngestionRegistry registry = new IngestionRegistry();
        AtomicInteger invocations = new AtomicInteger();
        Supplier<IngestionResult> supplier = () -> {
            invocations.incrementAndGet();
            return new IngestionResult("fp", 1, 1, 0, List.of(), false);
        };

        IngestionRegistry.CacheOutcome first = registry.getOrCompute("fp", supplier);
        IngestionRegistry.CacheOutcome second = registry.getOrCompute("fp", supplier);

        assertFalse(first.cached());
        assertTrue(second.cached());
        assertSame(first.result(), second.result());
        assertEquals(1, invocations.get());
    }

    @Test
    void forceComputeAlwaysRecomputesAndOverwritesCache() {
        IngestionRegistry registry = new IngestionRegistry();
        AtomicInteger invocations = new AtomicInteger();
        Supplier<IngestionResult> supplier = () -> {
            int call = invocations.incrementAndGet();
            return new IngestionResult("fp", call, call, 0, List.of(), false);
        };

        registry.getOrCompute("fp", supplier);
        IngestionResult forced = registry.forceCompute("fp", supplier);
        IngestionRegistry.CacheOutcome afterForce = registry.getOrCompute("fp", supplier);

        assertEquals(2, invocations.get());
        assertEquals(2, forced.totalLines());
        assertTrue(afterForce.cached());
        assertSame(forced, afterForce.result());
    }

    @Test
    void concurrentCallsForSameFingerprintComputeExactlyOnce() throws Exception {
        IngestionRegistry registry = new IngestionRegistry();
        AtomicInteger invocations = new AtomicInteger();
        CountDownLatch releaseLatch = new CountDownLatch(1);
        Supplier<IngestionResult> slowSupplier = () -> {
            invocations.incrementAndGet();
            try {
                releaseLatch.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new IngestionResult("fp", 1, 1, 0, List.of(), false);
        };

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<IngestionRegistry.CacheOutcome> first =
                    executor.submit(() -> registry.getOrCompute("fp", slowSupplier));
            Thread.sleep(100);
            Future<IngestionRegistry.CacheOutcome> second =
                    executor.submit(() -> registry.getOrCompute("fp", slowSupplier));
            Thread.sleep(100);
            releaseLatch.countDown();

            IngestionRegistry.CacheOutcome firstOutcome = first.get(2, TimeUnit.SECONDS);
            IngestionRegistry.CacheOutcome secondOutcome = second.get(2, TimeUnit.SECONDS);

            assertEquals(1, invocations.get());
            assertSame(firstOutcome.result(), secondOutcome.result());
        } finally {
            executor.shutdown();
        }
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionRegistryTest`
Expected: FAIL — `IngestionRegistry` does not exist yet.

- [ ] **Step 4: Implement `IngestionRegistry`**

```java
package com.pfm.ingestion;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class IngestionRegistry {

    public record CacheOutcome(IngestionResult result, boolean cached) {
    }

    private final ConcurrentHashMap<String, IngestionResult> cache = new ConcurrentHashMap<>();

    public CacheOutcome getOrCompute(String fingerprint, Supplier<IngestionResult> computation) {
        AtomicBoolean computed = new AtomicBoolean(false);
        IngestionResult result = cache.computeIfAbsent(fingerprint, fp -> {
            computed.set(true);
            return computation.get();
        });
        return new CacheOutcome(result, !computed.get());
    }

    public IngestionResult forceCompute(String fingerprint, Supplier<IngestionResult> computation) {
        IngestionResult result = computation.get();
        cache.put(fingerprint, result);
        return result;
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionRegistryTest`
Expected: `BUILD SUCCESS`, 3/3 passing.

- [ ] **Step 6: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/IngestionResult.java \
        ingestion-service/src/main/java/com/pfm/ingestion/IngestionRegistry.java \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionRegistryTest.java
git commit -m "feat(ingestion-service): add IngestionResult and IngestionRegistry dedup cache"
```

---

### Task 5: Kafka producer and topic configuration

**Files:**
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaProducerConfig.java`
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaTopicConfig.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaConfigTest.java`

**Interfaces:**
- Consumes: `IngestionProperties` (Task 1), `com.pfm.common.domain.FutureTransaction` (existing).
- Produces: a `KafkaTemplate<String, FutureTransaction>` bean and a `NewTopic` bean named `future-transactions` (3 partitions, replication factor 1), both consumed by `IngestionService` (Task 6).

- [ ] **Step 1: Write the failing config test**

```java
package com.pfm.ingestion.kafka;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = "spring.kafka.admin.auto-create=false")
class KafkaConfigTest {

    @Autowired
    KafkaTemplate<String, FutureTransaction> kafkaTemplate;

    @Autowired
    NewTopic futureTransactionsTopic;

    @Test
    void kafkaTemplateBeanExists() {
        assertNotNull(kafkaTemplate);
    }

    @Test
    void topicIsConfiguredWithThreePartitionsAndReplicationFactorOne() {
        assertEquals("future-transactions", futureTransactionsTopic.name());
        assertEquals(3, futureTransactionsTopic.numPartitions());
        assertEquals(1, futureTransactionsTopic.replicationFactor());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -pl ingestion-service -am test -Dtest=KafkaConfigTest`
Expected: FAIL — no `KafkaTemplate<String, FutureTransaction>` or `NewTopic` bean defined yet.

- [ ] **Step 3: Implement `KafkaProducerConfig`**

```java
package com.pfm.ingestion.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, FutureTransaction> futureTransactionProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        return new DefaultKafkaProducerFactory<>(configProps, new StringSerializer(),
                new JsonSerializer<FutureTransaction>(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, FutureTransaction> futureTransactionKafkaTemplate(
            ProducerFactory<String, FutureTransaction> futureTransactionProducerFactory) {
        return new KafkaTemplate<>(futureTransactionProducerFactory);
    }
}
```

- [ ] **Step 4: Implement `KafkaTopicConfig`**

```java
package com.pfm.ingestion.kafka;

import com.pfm.ingestion.IngestionProperties;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic futureTransactionsTopic(IngestionProperties properties) {
        return TopicBuilder.name(properties.topic())
                .partitions(3)
                .replicas(1)
                .build();
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q -pl ingestion-service -am test -Dtest=KafkaConfigTest`
Expected: `BUILD SUCCESS`, 2/2 passing (still no live broker needed — bean creation only).

- [ ] **Step 6: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaProducerConfig.java \
        ingestion-service/src/main/java/com/pfm/ingestion/kafka/KafkaTopicConfig.java \
        ingestion-service/src/test/java/com/pfm/ingestion/kafka/KafkaConfigTest.java
git commit -m "feat(ingestion-service): configure Kafka producer (JSON, acks=all, idempotent) and topic"
```

---

### Task 6: `IngestionService`

**Files:**
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionFileNotFoundException.java`
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/KafkaPublishException.java`
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionService.java`
- Create (test fixture): `ingestion-service/src/test/resources/small-sample.txt`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceTest.java`

**Interfaces:**
- Consumes: `FutureTransactionParser` (common), `KafkaTemplate<String, FutureTransaction>` (Task 5), `IngestionRegistry` (Task 4), `IngestionProperties` (Task 1), `KafkaKeyBuilder.buildKey` (Task 3).
- Produces: `IngestionService.ingest(boolean force)` → `IngestionResult`, consumed by `IngestionController` (Task 7).

**Fixture:** `small-sample.txt` is 2 real lines from `sample-data/Input.txt` (both parse to the same client/product, key `CL432100020001|SGXFUNK2010-09-10`) plus 1 truncated real line that fails to parse. This was verified directly against the real `FutureTransactionParser`: 3 lines in → 2 records, 1 error (`"Field 'clearingFeeRaw' needs 101 characters but line has 100"`, line 3).

- [ ] **Step 1: Generate the fixture from the real sample file**

```bash
mkdir -p ingestion-service/src/test/resources
head -2 sample-data/Input.txt > ingestion-service/src/test/resources/small-sample.txt
head -1 sample-data/Input.txt | cut -c1-100 >> ingestion-service/src/test/resources/small-sample.txt
```

Verify it: `awk '{ print length }' ingestion-service/src/test/resources/small-sample.txt` should print `176`, `176`, `100` (the first two lines carry a trailing `\r` from the source file's CRLF endings, which `Files.readAllLines` strips on read, same as `common`'s `GoldenSampleFileTest`).

- [ ] **Step 2: Create the exception types**

`ingestion-service/src/main/java/com/pfm/ingestion/IngestionFileNotFoundException.java`:

```java
package com.pfm.ingestion;

import java.nio.file.Path;

public class IngestionFileNotFoundException extends RuntimeException {

    public IngestionFileNotFoundException(Path path) {
        super("Ingestion file not found: " + path.toAbsolutePath());
    }
}
```

`ingestion-service/src/main/java/com/pfm/ingestion/KafkaPublishException.java`:

```java
package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;

import java.util.List;

public class KafkaPublishException extends RuntimeException {

    private final List<ParseError> failures;

    public KafkaPublishException(String message, List<ParseError> failures) {
        super(message);
        this.failures = failures;
    }

    public List<ParseError> failures() {
        return failures;
    }
}
```

- [ ] **Step 3: Write the failing `IngestionService` tests**

```java
package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.FutureTransactionParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class IngestionServiceTest {

    private KafkaTemplate<String, FutureTransaction> kafkaTemplate;
    private IngestionService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws URISyntaxException {
        kafkaTemplate = mock(KafkaTemplate.class);
        Path fixture = Path.of(getClass().getClassLoader().getResource("small-sample.txt").toURI());
        IngestionProperties properties = new IngestionProperties(fixture.toString(), "future-transactions");
        service = new IngestionService(new FutureTransactionParser(), kafkaTemplate, new IngestionRegistry(), properties);
    }

    @Test
    void ingestsFileAndPublishesEachParsedRecordKeyedByClientAndProduct() {
        when(kafkaTemplate.send(anyString(), anyString(), any(FutureTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        IngestionResult result = service.ingest(false);

        assertEquals(3, result.totalLines());
        assertEquals(2, result.published());
        assertEquals(1, result.skipped());
        assertEquals(1, result.errors().size());
        assertEquals(3, result.errors().get(0).lineNumber());
        assertFalse(result.cached());
        verify(kafkaTemplate, times(2))
                .send(eq("future-transactions"), eq("CL432100020001|SGXFUNK2010-09-10"), any(FutureTransaction.class));
    }

    @Test
    void secondIngestOfSameFileIsServedFromCacheWithoutRepublishing() {
        when(kafkaTemplate.send(anyString(), anyString(), any(FutureTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        IngestionResult first = service.ingest(false);
        IngestionResult second = service.ingest(false);

        assertFalse(first.cached());
        assertTrue(second.cached());
        assertEquals(first.published(), second.published());
        verify(kafkaTemplate, times(2)).send(anyString(), anyString(), any(FutureTransaction.class));
    }

    @Test
    void forceBypassesCacheAndRepublishes() {
        when(kafkaTemplate.send(anyString(), anyString(), any(FutureTransaction.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        service.ingest(false);
        IngestionResult forced = service.ingest(true);

        assertFalse(forced.cached());
        verify(kafkaTemplate, times(4)).send(anyString(), anyString(), any(FutureTransaction.class));
    }

    @Test
    void throwsKafkaPublishExceptionWhenAllSendsFail() {
        CompletableFuture<SendResult<String, FutureTransaction>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));
        when(kafkaTemplate.send(anyString(), anyString(), any(FutureTransaction.class))).thenReturn(failed);

        KafkaPublishException exception = assertThrows(KafkaPublishException.class, () -> service.ingest(false));
        assertTrue(exception.getMessage().contains("2"));
    }

    @Test
    void throwsIngestionFileNotFoundExceptionForMissingFile() {
        IngestionProperties missing = new IngestionProperties("does-not-exist.txt", "future-transactions");
        IngestionService missingFileService =
                new IngestionService(new FutureTransactionParser(), kafkaTemplate, new IngestionRegistry(), missing);

        assertThrows(IngestionFileNotFoundException.class, () -> missingFileService.ingest(false));
    }
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionServiceTest`
Expected: FAIL — `IngestionService` does not exist yet.

- [ ] **Step 5: Implement `IngestionService`**

```java
package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.FutureTransactionParser;
import com.pfm.common.domain.ParseResult;
import com.pfm.common.fixedwidth.ParseError;
import com.pfm.ingestion.kafka.KafkaKeyBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class IngestionService {

    private final FutureTransactionParser parser;
    private final KafkaTemplate<String, FutureTransaction> kafkaTemplate;
    private final IngestionRegistry registry;
    private final IngestionProperties properties;

    public IngestionService(FutureTransactionParser parser,
                             KafkaTemplate<String, FutureTransaction> kafkaTemplate,
                             IngestionRegistry registry,
                             IngestionProperties properties) {
        this.parser = parser;
        this.kafkaTemplate = kafkaTemplate;
        this.registry = registry;
        this.properties = properties;
    }

    public IngestionResult ingest(boolean force) {
        Path path = Path.of(properties.filePath());
        if (!Files.exists(path)) {
            throw new IngestionFileNotFoundException(path);
        }

        String fingerprint;
        try {
            fingerprint = FileFingerprint.compute(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (force) {
            return registry.forceCompute(fingerprint, () -> runIngestion(path, fingerprint));
        }

        IngestionRegistry.CacheOutcome outcome = registry.getOrCompute(fingerprint, () -> runIngestion(path, fingerprint));
        return outcome.result().withCached(outcome.cached());
    }

    private IngestionResult runIngestion(Path path, String fingerprint) {
        List<String> lines;
        try {
            lines = Files.readAllLines(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        ParseResult parseResult = parser.parseAll(lines);

        List<ParseError> sendFailures = new ArrayList<>();
        int published = 0;
        for (FutureTransaction transaction : parseResult.records()) {
            String key = KafkaKeyBuilder.buildKey(transaction);
            try {
                kafkaTemplate.send(properties.topic(), key, transaction).get(10, TimeUnit.SECONDS);
                published++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sendFailures.add(new ParseError(-1, key, "Kafka send interrupted: " + e.getMessage()));
            } catch (ExecutionException | TimeoutException e) {
                sendFailures.add(new ParseError(-1, key, "Kafka send failed: " + e.getMessage()));
            }
        }

        List<ParseError> allErrors = new ArrayList<>(parseResult.errors());
        allErrors.addAll(sendFailures);

        if (!parseResult.records().isEmpty() && published == 0) {
            throw new KafkaPublishException(
                    "All " + parseResult.records().size() + " parsed records failed to publish to topic '"
                            + properties.topic() + "'",
                    allErrors);
        }

        return new IngestionResult(fingerprint, lines.size(), published, lines.size() - published, allErrors, false);
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionServiceTest`
Expected: `BUILD SUCCESS`, 5/5 passing.

- [ ] **Step 7: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/IngestionFileNotFoundException.java \
        ingestion-service/src/main/java/com/pfm/ingestion/KafkaPublishException.java \
        ingestion-service/src/main/java/com/pfm/ingestion/IngestionService.java \
        ingestion-service/src/test/resources/small-sample.txt \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceTest.java
git commit -m "feat(ingestion-service): add IngestionService orchestration"
```

---

### Task 7: `IngestionController`

**Files:**
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionControllerTest.java`

**Interfaces:**
- Consumes: `IngestionService.ingest(boolean)` (Task 6), `IngestionResult`/`KafkaPublishException`/`IngestionFileNotFoundException` (Task 6).
- Produces: `POST /api/ingest[?force=true]` → 200 with `IngestionResult` body, 502 on `KafkaPublishException`, 404 on `IngestionFileNotFoundException`.

- [ ] **Step 1: Write the failing controller tests**

```java
package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(IngestionController.class)
class IngestionControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    IngestionService ingestionService;

    @Test
    void postIngestReturnsResultFromService() throws Exception {
        IngestionResult result = new IngestionResult(
                "fp-1", 3, 2, 1, List.of(new ParseError(3, "bad", "too short")), false);
        when(ingestionService.ingest(false)).thenReturn(result);

        mockMvc.perform(post("/api/ingest"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fingerprint").value("fp-1"))
                .andExpect(jsonPath("$.totalLines").value(3))
                .andExpect(jsonPath("$.published").value(2))
                .andExpect(jsonPath("$.skipped").value(1))
                .andExpect(jsonPath("$.cached").value(false));

        verify(ingestionService).ingest(false);
    }

    @Test
    void postIngestWithForceParamPassesForceFlagThrough() throws Exception {
        IngestionResult result = new IngestionResult("fp-1", 3, 3, 0, List.of(), false);
        when(ingestionService.ingest(true)).thenReturn(result);

        mockMvc.perform(post("/api/ingest").param("force", "true"))
                .andExpect(status().isOk());

        verify(ingestionService).ingest(true);
    }

    @Test
    void kafkaPublishFailureReturns502() throws Exception {
        when(ingestionService.ingest(false)).thenThrow(new KafkaPublishException("all failed", List.of()));

        mockMvc.perform(post("/api/ingest"))
                .andExpect(status().isBadGateway());
    }

    @Test
    void missingFileReturns404() throws Exception {
        when(ingestionService.ingest(false))
                .thenThrow(new IngestionFileNotFoundException(Path.of("missing.txt")));

        mockMvc.perform(post("/api/ingest"))
                .andExpect(status().isNotFound());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionControllerTest`
Expected: FAIL — `IngestionController` does not exist yet.

- [ ] **Step 3: Implement `IngestionController`**

```java
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionControllerTest`
Expected: `BUILD SUCCESS`, 4/4 passing.

- [ ] **Step 5: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/IngestionController.java \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionControllerTest.java
git commit -m "feat(ingestion-service): add POST /api/ingest REST endpoint"
```

---

### Task 8: End-to-end Testcontainers integration test

**Files:**
- Create (test fixture): `ingestion-service/src/test/resources/Input.txt` (copy of `sample-data/Input.txt`, same technique `common` used for its golden test)
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java`

**Interfaces:**
- Consumes: the full stack built in Tasks 1–7, plus a real Kafka broker started by Testcontainers.
- Produces: nothing new — this is the acceptance test proving the whole slice works together.

**Expected numbers** (verified earlier by running the real parser over the full sample file): 717 lines, 717 records, 0 errors. Across the three POSTs in this test (ingest, repeat, forced re-ingest), the topic receives `717 + 0 + 717 = 1434` messages total.

- [ ] **Step 1: Copy the full sample file into test resources**

```bash
cp sample-data/Input.txt ingestion-service/src/test/resources/Input.txt
```

- [ ] **Step 2: Write the test**

```java
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
```

- [ ] **Step 3: Run the test**

Run: `mvn -q -pl ingestion-service -am test -Dtest=IngestionEndToEndTest`
Expected: `BUILD SUCCESS`, 1/1 passing. Requires Docker running locally (Testcontainers). First run pulls `apache/kafka:3.9.2`, which takes longer than subsequent runs.

- [ ] **Step 4: Commit**

```bash
git add ingestion-service/src/test/resources/Input.txt \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java
git commit -m "test(ingestion-service): add Testcontainers end-to-end ingestion test"
```

---

### Task 9: Local dev Kafka + docs

**Files:**
- Create: `docker-compose.yml` (repo root)
- Modify: `ingestion-service/README.md`
- Modify: `README.md` (repo root)

**Interfaces:**
- Consumes: nothing new.
- Produces: a runnable local dev stack (`docker compose up`) and accurate docs — no code interfaces.

- [ ] **Step 1: Create `docker-compose.yml`**

Adapted from Apache Kafka's own official single-node KRaft example
(`docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml` in the
`apache/kafka` repo), pinned to the same `apache/kafka:3.9.2` image used by the
Testcontainers test:

```yaml
services:
  kafka:
    image: apache/kafka:3.9.2
    hostname: broker
    container_name: pfm-kafka
    ports:
      - '9092:9092'
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT_HOST://localhost:9092,PLAINTEXT://broker:19092'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
      KAFKA_LISTENERS: 'CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      CLUSTER_ID: '4L6g3nShT-eMCtK--X86sw'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
```

- [ ] **Step 2: Update `ingestion-service/README.md`**

Replace its content with:

```markdown
# ingestion-service

Spring Boot service that reads the System A fixed-width file (`Input.txt`) using the
parser in [`common`](../common/) and publishes one Kafka event per transaction record
to the `future-transactions` topic.

Stands in for the "Kafka streaming input instead of file" scenario called out in the
requirements — this service is the producer side of that pipeline.

Design decisions: [docs/superpowers/specs/2026-08-09-ingestion-service-design.md](../docs/superpowers/specs/2026-08-09-ingestion-service-design.md).

## Running locally

```bash
docker compose up -d          # starts a local Kafka broker on localhost:9092
mvn -pl ingestion-service -am spring-boot:run
```

By default it reads `sample-data/Input.txt` (relative to the repo root). Override with
`INGESTION_FILE_PATH=/path/to/file`.

## API

- `POST /api/ingest` — parses the configured file and publishes each record to Kafka.
  Returns a JSON body: `{fingerprint, totalLines, published, skipped, errors, cached}`.
  Calling it again for the same (unchanged) file returns the cached result without
  republishing (`cached: true`).
- `POST /api/ingest?force=true` — bypasses the cache and republishes even if this exact
  file was already ingested.

```bash
curl -X POST http://localhost:8081/api/ingest
```
```

- [ ] **Step 3: Update root `README.md`**

Change the `## Status` section from:

```markdown
## Status

Repo structure and sample output only so far — service implementations are in progress.
See `CLAUDE.md` for AI-assistance context on this project.
```

to:

```markdown
## Status

- `common` — done: fixed-width parser + domain model.
- `ingestion-service` — done: `POST /api/ingest` reads the file and publishes to Kafka
  (JSON, keyed by client+product, idempotent per file version). See its
  [README](ingestion-service/README.md) for usage.
- `processing-service`, `frontend`, `k8s` — not started.

See `CLAUDE.md` for AI-assistance context on this project.
```

- [ ] **Step 4: Verify the whole module builds clean**

Run: `mvn -q -pl ingestion-service -am verify`
Expected: `BUILD SUCCESS`, all tests from Tasks 1–8 passing (requires Docker for Task 8's Testcontainers test).

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml ingestion-service/README.md README.md
git commit -m "docs(ingestion-service): add local dev compose file and usage docs"
```

## Out of scope for this slice

- `processing-service` (Kafka Streams aggregation, REST reporting API).
- Angular frontend, Kubernetes manifests.
- A dead-letter topic for unparseable lines (per design decision #9, these are reported
  in the response body and logged, not routed to a separate topic).
