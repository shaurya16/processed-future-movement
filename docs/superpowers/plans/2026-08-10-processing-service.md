# processing-service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `processing-service`, a Spring Boot / Kafka Streams service that consumes `future-transactions`, maintains an idempotent running per-(Client_Information, Product_Information) net-quantity aggregate, and exposes it via `GET /api/report` (JSON) and `GET /api/report/csv` — while also closing the dedup gap this requires reopening `common` and `ingestion-service` for.

**Architecture:** `ingestion-service` gains a per-record `transactionId` (SHA-256 of file-content-hash + line-number) attached as a Kafka message header. `processing-service`'s Kafka Streams topology reads that header in a custom `Processor` stage, drops anything already seen (persistent, changelog-backed dedup store), then `groupByKey().aggregate()`s the survivors into a persistent `net-quantity-store`. REST endpoints do Interactive Queries against that store.

**Tech Stack:** Spring Boot 3.5.4, Spring Kafka 3.3.8 (managed by the parent POM), Kafka Streams 3.9.x (managed to match the already-resolved `kafka-clients:3.9.1`), Jackson (`JavaTimeModule`, no timestamp dates), JUnit 5, Mockito, `kafka-streams-test-utils` (`TopologyTestDriver`), Testcontainers Kafka.

Full design rationale: [docs/superpowers/specs/2026-08-09-processing-service-design.md](../specs/2026-08-09-processing-service-design.md).

## Global Constraints

- Java 21, `maven.compiler.release=21` (root `pom.xml`).
- `common`'s `FutureTransaction` domain record and its JSON wire schema must NOT change — the `transactionId` travels as a Kafka header, never as a value field.
- All new Kafka Streams state stores are persistent/changelog-backed (the `Materialized.as(name)` / `Stores.persistentKeyValueStore(name)` default) — never in-memory. An in-memory dedup store would forget every `transactionId` on restart and defeat the whole point of this design.
- `processing-service` runs as a single instance (`replicas: 1` is a constraint on the future k8s manifest, same as `ingestion-service`) — no cross-instance Interactive Query routing is built.
- REST report field names are exactly `Client_Information`, `Product_Information`, `Total_Transaction_Amount` (matching `docs/file-spec.md` / `sample-output/Output.csv`), on both the JSON and CSV endpoints.
- No new CSV library — `Client_Information`/`Product_Information` are fixed positional codes that can't contain commas or quotes, so manual string building is sufficient.
- Every Maven dependency version added for `processing-service` relies on `spring-boot-starter-parent:3.5.4`'s dependency management (no explicit `<version>` needed for `spring-kafka`, `kafka-streams`, `kafka-streams-test-utils`, `testcontainers-bom` already pins Testcontainers at `1.21.3` the way `ingestion-service`'s `pom.xml` already does).

---

## Task 1: `common` — carry line numbers through parsing via `ParsedRecord`

**Files:**
- Create: `common/src/main/java/com/pfm/common/domain/ParsedRecord.java`
- Modify: `common/src/main/java/com/pfm/common/domain/ParseResult.java`
- Modify: `common/src/main/java/com/pfm/common/domain/FutureTransactionParser.java:29` (the `records.add(parse(...))` line inside `parseAll`)
- Modify (to make it compile again): `common/src/test/java/com/pfm/common/domain/FutureTransactionParserTest.java`
- Test: `common/src/test/java/com/pfm/common/domain/FutureTransactionParserTest.java` (existing file, updated in place)

**Interfaces:**
- Produces: `record ParsedRecord(int lineNumber, FutureTransaction transaction)`; `ParseResult.records()` now returns `List<ParsedRecord>` instead of `List<FutureTransaction>`. Everything downstream (`ingestion-service`'s `IngestionService`, Task 3 of this plan) consumes this new shape.

A successfully-parsed record's original line number is currently discarded by `parseAll` — needed later so `ingestion-service` can compute `transactionId = sha256Hex(contentHash + ":" + lineNumber)` per record.

- [ ] **Step 1: Update the existing test to use the new `ParsedRecord` API (it will not compile yet)**

Replace the full contents of `common/src/test/java/com/pfm/common/domain/FutureTransactionParserTest.java`:

```java
package com.pfm.common.domain;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FutureTransactionParserTest {

    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    private static final String LINE_13 =
        "315CL  432100030001FCC   FUCME N1    20100910JPY01S 0000000000 0000000003000000000000DUSD000000000015DUSD000000000000DJPY20100819059475      000308000093300000000             O";

    private static final String TRUNCATED_LINE = "315CL";

    private final FutureTransactionParser parser = new FutureTransactionParser();

    @Test
    void parseAllReturnsAllRecordsWhenEveryLineIsValid() {
        ParseResult result = parser.parseAll(List.of(LINE_1, LINE_13));

        assertEquals(2, result.records().size());
        assertTrue(result.errors().isEmpty());
        assertEquals("0002", result.records().get(0).transaction().accountNumber());
        assertEquals(1, result.records().get(0).lineNumber());
        assertEquals("0003", result.records().get(1).transaction().accountNumber());
        assertEquals(2, result.records().get(1).lineNumber());
    }

    @Test
    void parseAllSkipsBadLineAndCollectsError() {
        ParseResult result = parser.parseAll(List.of(LINE_1, TRUNCATED_LINE));

        assertEquals(1, result.records().size());
        assertEquals("0002", result.records().get(0).transaction().accountNumber());
        assertEquals(1, result.records().get(0).lineNumber());
        assertEquals(1, result.errors().size());
        assertEquals(2, result.errors().get(0).lineNumber());
        assertEquals(TRUNCATED_LINE, result.errors().get(0).rawLine());
    }

    @Test
    void parseAllRecoversAfterABadLineAndKeepsParsingSubsequentGoodLines() {
        ParseResult result = parser.parseAll(List.of(LINE_1, TRUNCATED_LINE, LINE_13));

        assertEquals(2, result.records().size());
        assertEquals("0002", result.records().get(0).transaction().accountNumber());
        assertEquals(1, result.records().get(0).lineNumber());
        assertEquals("0003", result.records().get(1).transaction().accountNumber());
        // LINE_13 is the 3rd input line, so its ParsedRecord must carry lineNumber 3,
        // not 2 (the index it would have in a flat List<FutureTransaction>) — this is
        // exactly the gap ParsedRecord exists to close.
        assertEquals(3, result.records().get(1).lineNumber());
        assertEquals(1, result.errors().size());
        assertEquals(2, result.errors().get(0).lineNumber());
    }

    @Test
    void parseAllReturnsOnlyErrorsWhenEveryLineIsBad() {
        ParseResult result = parser.parseAll(List.of(TRUNCATED_LINE, "x"));

        assertTrue(result.records().isEmpty());
        assertEquals(2, result.errors().size());
        assertEquals(1, result.errors().get(0).lineNumber());
        assertEquals(2, result.errors().get(1).lineNumber());
    }

    @Test
    void strictParseThrowsOnBadLine() {
        org.junit.jupiter.api.Assertions.assertThrows(
            com.pfm.common.fixedwidth.FixedWidthParseException.class,
            () -> parser.parse(TRUNCATED_LINE, 1));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `mvn -pl common test -Dtest=FutureTransactionParserTest`
Expected: BUILD FAILURE — compile error, `cannot find symbol: method transaction()` / `method lineNumber()` (or similar), since `ParsedRecord` doesn't exist yet.

- [ ] **Step 3: Create `ParsedRecord`**

```java
package com.pfm.common.domain;

/** A successfully-parsed record paired with the 1-indexed source line it came from. */
public record ParsedRecord(int lineNumber, FutureTransaction transaction) {
}
```

- [ ] **Step 4: Change `ParseResult.records()` to `List<ParsedRecord>`**

Replace the full contents of `common/src/main/java/com/pfm/common/domain/ParseResult.java`:

```java
package com.pfm.common.domain;

import com.pfm.common.fixedwidth.ParseError;

import java.util.List;

public record ParseResult(List<ParsedRecord> records, List<ParseError> errors) {
}
```

- [ ] **Step 5: Update `FutureTransactionParser.parseAll` to wrap each record**

In `common/src/main/java/com/pfm/common/domain/FutureTransactionParser.java`, change the `parseAll` method body:

```java
    public ParseResult parseAll(List<String> lines) {
        List<ParsedRecord> records = new ArrayList<>();
        List<ParseError> errors = new ArrayList<>();

        for (int i = 0; i < lines.size(); i++) {
            int lineNumber = i + 1;
            try {
                records.add(new ParsedRecord(lineNumber, parse(lines.get(i), lineNumber)));
            } catch (FixedWidthParseException e) {
                errors.add(new ParseError(e.lineNumber(), e.rawLine(), e.reason()));
            }
        }

        return new ParseResult(records, errors);
    }
```

(Only the `List<FutureTransaction> records` declaration and the `records.add(...)` line change; imports and the rest of the class are unchanged.)

- [ ] **Step 6: Run the common module's full test suite**

Run: `mvn -pl common test`
Expected: BUILD SUCCESS. `GoldenSampleFileTest` needs no changes — it only calls `result.records().size()` and `result.errors()`, neither of which changed shape.

- [ ] **Step 7: Commit**

```bash
git add common/src/main/java/com/pfm/common/domain/ParsedRecord.java \
        common/src/main/java/com/pfm/common/domain/ParseResult.java \
        common/src/main/java/com/pfm/common/domain/FutureTransactionParser.java \
        common/src/test/java/com/pfm/common/domain/FutureTransactionParserTest.java
git commit -m "feat(common): carry source line numbers through parseAll via ParsedRecord"
```

---

## Task 2: `ingestion-service` — content-hash and transaction-ID utilities

**Files:**
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/ContentHash.java`
- Create: `ingestion-service/src/main/java/com/pfm/ingestion/TransactionIdBuilder.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/ContentHashTest.java`
- Test: `ingestion-service/src/test/java/com/pfm/ingestion/TransactionIdBuilderTest.java`

**Interfaces:**
- Produces: `ContentHash.compute(Path path) throws IOException` → hex-encoded SHA-256 of the file's raw bytes. `TransactionIdBuilder.build(String contentHash, int lineNumber)` → hex-encoded SHA-256 of `contentHash + ":" + lineNumber`. Task 4 (`IngestionService`) calls both.
- Consumes: nothing from earlier tasks.

Verified concrete values (computed independently with `shasum -a 256` against the real `ingestion-service/src/test/resources/small-sample.txt` fixture, confirmed to be exactly 64 hex characters each):
- Content hash of `small-sample.txt`: `273e42bc7eb45afc4a54e3b77b251dd59cbfd48bb1bebd6842eb6f59504992a9`
- `TransactionIdBuilder.build(thatHash, 1)`: `e67676947c605bc5218aa1812fe5ec3c5e9d2ff34481343f2fc22633fc833413`
- `TransactionIdBuilder.build(thatHash, 2)`: `cb418a2ad4a9f87431544d1ebd2d25405f7ee3ebb31972524abb8f7678459806`

(Note: `small-sample.txt`'s first two lines are byte-identical `FutureTransaction` content — these two transaction IDs are deliberately different despite that, which is the property this whole design depends on.)

- [ ] **Step 1: Write the failing tests**

`ingestion-service/src/test/java/com/pfm/ingestion/ContentHashTest.java`:

```java
package com.pfm.ingestion;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ContentHashTest {

    @Test
    void computesTheKnownSha256HexDigestOfTheSmallSampleFixture() throws URISyntaxException, IOException {
        Path fixture = Path.of(getClass().getClassLoader().getResource("small-sample.txt").toURI());

        String hash = ContentHash.compute(fixture);

        assertEquals("273e42bc7eb45afc4a54e3b77b251dd59cbfd48bb1bebd6842eb6f59504992a9", hash);
        assertEquals(64, hash.length());
    }

    @Test
    void sameFileContentProducesTheSameHash() throws IOException {
        Path a = Files.createTempFile("content-hash-a", ".txt");
        Path b = Files.createTempFile("content-hash-b", ".txt");
        Files.writeString(a, "identical content");
        Files.writeString(b, "identical content");

        assertEquals(ContentHash.compute(a), ContentHash.compute(b));
    }

    @Test
    void differentFileContentProducesADifferentHash() throws IOException {
        Path a = Files.createTempFile("content-hash-a", ".txt");
        Path b = Files.createTempFile("content-hash-b", ".txt");
        Files.writeString(a, "hello");
        Files.writeString(b, "goodbye");

        assertNotEquals(ContentHash.compute(a), ContentHash.compute(b));
    }
}
```

`ingestion-service/src/test/java/com/pfm/ingestion/TransactionIdBuilderTest.java`:

```java
package com.pfm.ingestion;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TransactionIdBuilderTest {

    private static final String CONTENT_HASH =
            "273e42bc7eb45afc4a54e3b77b251dd59cbfd48bb1bebd6842eb6f59504992a9";

    @Test
    void buildsTheKnownTransactionIdForLine1() {
        assertEquals("e67676947c605bc5218aa1812fe5ec3c5e9d2ff34481343f2fc22633fc833413",
                TransactionIdBuilder.build(CONTENT_HASH, 1));
    }

    @Test
    void buildsTheKnownTransactionIdForLine2() {
        assertEquals("cb418a2ad4a9f87431544d1ebd2d25405f7ee3ebb31972524abb8f7678459806",
                TransactionIdBuilder.build(CONTENT_HASH, 2));
    }

    @Test
    void differentLineNumbersProduceDifferentIdsEvenForTheSameContentHash() {
        assertNotEquals(TransactionIdBuilder.build(CONTENT_HASH, 1), TransactionIdBuilder.build(CONTENT_HASH, 2));
    }

    @Test
    void sameContentHashAndLineNumberAlwaysProduceTheSameId() {
        assertEquals(TransactionIdBuilder.build(CONTENT_HASH, 5), TransactionIdBuilder.build(CONTENT_HASH, 5));
    }

    @Test
    void differentContentHashesProduceDifferentIdsForTheSameLineNumber() {
        String otherHash = "0000000000000000000000000000000000000000000000000000000000000000".substring(0, 64);
        assertNotEquals(TransactionIdBuilder.build(CONTENT_HASH, 1), TransactionIdBuilder.build(otherHash, 1));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -pl ingestion-service test -Dtest=ContentHashTest,TransactionIdBuilderTest`
Expected: BUILD FAILURE — compile error, `ContentHash`/`TransactionIdBuilder` don't exist yet.

- [ ] **Step 3: Implement `ContentHash`**

```java
package com.pfm.ingestion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class ContentHash {

    private ContentHash() {
    }

    public static String compute(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        return hex(sha256(bytes));
    }

    private static byte[] sha256(byte[] input) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(input);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is a mandatory algorithm on every JDK security provider; this
            // can only happen if the JVM itself is misconfigured.
            throw new UncheckedIOException(new IOException("SHA-256 not available", e));
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
```

- [ ] **Step 4: Implement `TransactionIdBuilder`**

```java
package com.pfm.ingestion;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class TransactionIdBuilder {

    private TransactionIdBuilder() {
    }

    public static String build(String contentHash, int lineNumber) {
        String basis = contentHash + ":" + lineNumber;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(basis.getBytes(StandardCharsets.UTF_8));
            return hex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn -pl ingestion-service test -Dtest=ContentHashTest,TransactionIdBuilderTest`
Expected: BUILD SUCCESS, all 9 tests pass.

- [ ] **Step 6: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/ContentHash.java \
        ingestion-service/src/main/java/com/pfm/ingestion/TransactionIdBuilder.java \
        ingestion-service/src/test/java/com/pfm/ingestion/ContentHashTest.java \
        ingestion-service/src/test/java/com/pfm/ingestion/TransactionIdBuilderTest.java
git commit -m "feat(ingestion-service): add content-hash and transaction-ID builder utilities"
```

---

## Task 3: `ingestion-service` — attach `transactionId` as a Kafka header

**Files:**
- Modify: `ingestion-service/src/main/java/com/pfm/ingestion/IngestionService.java`
- Modify (full rewrite): `ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceTest.java`

**Interfaces:**
- Consumes: `ParsedRecord` (Task 1), `ContentHash.compute(Path)` and `TransactionIdBuilder.build(String, int)` (Task 2).
- Produces: every `ProducerRecord` `IngestionService` sends now carries a `transactionId` header (UTF-8 bytes of the hex digest). `KafkaTemplate.send(ProducerRecord<String, FutureTransaction>)` replaces the previous 3-arg `send(topic, key, value)` call — this is a behavior/call-signature change later tasks don't depend on directly, but it changes what any future test mocking `kafkaTemplate` must stub.

`KafkaTemplate.send(ProducerRecord<K,V>)` (confirmed present on `org.springframework.kafka.core.KafkaTemplate` in `spring-kafka:3.3.8`) is used instead of the convenience 3-arg overload, because headers can only be attached via an explicit `ProducerRecord`.

- [ ] **Step 1: Rewrite `IngestionServiceTest` for the new `ProducerRecord`-based mocking (will not compile against the current `IngestionService`)**

Replace the full contents of `ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceTest.java`:

```java
package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.FutureTransactionParser;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
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

    @SuppressWarnings("unchecked")
    private void stubAllSendsSucceed() {
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
    }

    @SuppressWarnings("unchecked")
    private ArgumentCaptor<ProducerRecord<String, FutureTransaction>> captureProducerRecords() {
        return ArgumentCaptor.forClass(ProducerRecord.class);
    }

    private List<String> transactionIdsOf(List<ProducerRecord<String, FutureTransaction>> records) {
        return records.stream()
                .map(r -> new String(r.headers().lastHeader("transactionId").value(), StandardCharsets.UTF_8))
                .toList();
    }

    @Test
    void ingestsFileAndPublishesEachParsedRecordKeyedByClientAndProduct() {
        stubAllSendsSucceed();

        IngestionResult result = service.ingest(false);

        assertEquals(3, result.totalLines());
        assertEquals(2, result.published());
        assertEquals(1, result.skipped());
        assertEquals(1, result.errors().size());
        assertEquals(3, result.errors().get(0).lineNumber());
        assertFalse(result.cached());
        verify(kafkaTemplate, times(2)).send(argThat(record ->
                "future-transactions".equals(record.topic())
                        && "CL432100020001|SGXFUNK20100910".equals(record.key())));
    }

    @Test
    void publishesEachRecordWithADistinctTransactionIdHeaderDerivedFromContentHashAndLineNumber() {
        stubAllSendsSucceed();
        ArgumentCaptor<ProducerRecord<String, FutureTransaction>> captor = captureProducerRecords();

        service.ingest(false);

        verify(kafkaTemplate, times(2)).send(captor.capture());
        List<String> transactionIds = transactionIdsOf(captor.getAllValues());

        assertEquals(2, transactionIds.size());
        // small-sample.txt's first two lines are byte-identical FutureTransaction content;
        // these values must still differ because they're derived from (contentHash, lineNumber).
        assertEquals("e67676947c605bc5218aa1812fe5ec3c5e9d2ff34481343f2fc22633fc833413", transactionIds.get(0));
        assertEquals("cb418a2ad4a9f87431544d1ebd2d25405f7ee3ebb31972524abb8f7678459806", transactionIds.get(1));
    }

    @Test
    void transactionIdHeaderIsStableAcrossRepeatForcedIngestionOfUnchangedContent() {
        stubAllSendsSucceed();
        ArgumentCaptor<ProducerRecord<String, FutureTransaction>> captor = captureProducerRecords();

        service.ingest(true);
        service.ingest(true);

        verify(kafkaTemplate, times(4)).send(captor.capture());
        List<ProducerRecord<String, FutureTransaction>> sent = captor.getAllValues();

        List<String> firstCallIds = transactionIdsOf(sent.subList(0, 2));
        List<String> secondCallIds = transactionIdsOf(sent.subList(2, 4));
        assertEquals(firstCallIds, secondCallIds,
                "re-ingesting unchanged content via force=true must reproduce identical transaction IDs");
    }

    @Test
    void secondIngestOfSameFileIsServedFromCacheWithoutRepublishing() {
        stubAllSendsSucceed();

        IngestionResult first = service.ingest(false);
        IngestionResult second = service.ingest(false);

        assertFalse(first.cached());
        assertTrue(second.cached());
        assertEquals(first.published(), second.published());
        verify(kafkaTemplate, times(2)).send(any(ProducerRecord.class));
    }

    @Test
    void forceBypassesCacheAndRepublishes() {
        stubAllSendsSucceed();

        service.ingest(false);
        IngestionResult forced = service.ingest(true);

        assertFalse(forced.cached());
        verify(kafkaTemplate, times(4)).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void throwsKafkaPublishExceptionWhenAllSendsFail() {
        CompletableFuture<SendResult<String, FutureTransaction>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        KafkaPublishException exception = assertThrows(KafkaPublishException.class, () -> service.ingest(false));
        assertTrue(exception.getMessage().contains("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void abortsSendingAfterConsecutiveFailureThresholdToBoundWorstCaseBlockingTime() throws URISyntaxException {
        CompletableFuture<SendResult<String, FutureTransaction>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        Path fixture = Path.of(getClass().getClassLoader().getResource("many-valid-records-sample.txt").toURI());
        IngestionProperties properties = new IngestionProperties(fixture.toString(), "future-transactions");
        IngestionService manyRecordsService =
                new IngestionService(new FutureTransactionParser(), kafkaTemplate, new IngestionRegistry(), properties);

        // 6 valid records in the fixture, all sends fail: the service should stop after
        // MAX_CONSECUTIVE_SEND_FAILURES (5) rather than blocking up to 10s on every one.
        assertThrows(KafkaPublishException.class, () -> manyRecordsService.ingest(false));

        verify(kafkaTemplate, times(5)).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void partialKafkaSendFailureIsNotCachedSoTheNextNonForcedCallRetries() {
        CompletableFuture<SendResult<String, FutureTransaction>> success = CompletableFuture.completedFuture(null);
        CompletableFuture<SendResult<String, FutureTransaction>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("broker unreachable"));

        // Each ingest() attempt over small-sample.txt has 2 valid records: alternate
        // success/failure so every attempt is a genuine partial failure (1 published, 1 failed).
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(success, failed, success, failed);

        IngestionResult first = service.ingest(false);
        assertEquals(1, first.published());
        assertFalse(first.cached());
        assertTrue(first.errors().stream().anyMatch(e -> e.lineNumber() == -1),
                "expected a Kafka send failure (lineNumber -1) among the errors");

        // Decision (finding #5): a result with un-retried Kafka send failures must NOT be
        // cached, because a subsequent non-forced call must retry the failed records rather
        // than silently replaying a known-partial ingestion as if it fully succeeded.
        IngestionResult second = service.ingest(false);
        assertFalse(second.cached(), "a partial-failure result must not be served from cache");
        assertEquals(1, second.published());

        verify(kafkaTemplate, times(4)).send(any(ProducerRecord.class));
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

- [ ] **Step 2: Run the test to verify it fails to compile**

Run: `mvn -pl ingestion-service -am test -Dtest=IngestionServiceTest`
Expected: BUILD FAILURE — `IngestionService`'s constructor/`ingest` still compiles against the old 3-arg `send`, and `parseResult.records()` is still `List<FutureTransaction>` inside `IngestionService`, so this won't yet build against `ParsedRecord` either. (If Task 1/2 aren't yet visible because `common`/nothing changed there, the failure here is specifically about `ProducerRecord` mocking not matching the still-unmodified `IngestionService.runIngestion`.)

- [ ] **Step 3: Update `IngestionService`**

In `ingestion-service/src/main/java/com/pfm/ingestion/IngestionService.java`, replace the full file contents:

```java
package com.pfm.ingestion;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.common.domain.FutureTransactionParser;
import com.pfm.common.domain.ParseResult;
import com.pfm.common.domain.ParsedRecord;
import com.pfm.common.fixedwidth.ParseError;
import com.pfm.ingestion.kafka.KafkaKeyBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class IngestionService {

    /**
     * Bounds worst-case blocking time when the broker is unreachable/degraded: after this
     * many consecutive Kafka send failures we stop attempting further sends for this batch
     * rather than blocking up to 10s per remaining record (which for a 717-record file could
     * hold an HTTP request open for close to two hours).
     */
    private static final int MAX_CONSECUTIVE_SEND_FAILURES = 5;

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

        IngestionRegistry.CacheOutcome outcome = registry.getOrCompute(
                fingerprint,
                () -> runIngestion(path, fingerprint),
                IngestionService::hasNoKafkaSendFailures);
        return outcome.result().withCached(outcome.cached());
    }

    /**
     * A result is only safe to cache (and serve to future non-forced requests without
     * republishing) when every parsed record actually made it to Kafka. Parse errors are
     * deterministic for a given file and caching them is fine; Kafka send failures (tagged
     * with lineNumber == -1, see runIngestion below) are transient infrastructure failures,
     * and caching a batch that has known un-retried send failures would mean a financial
     * pipeline silently treats a partially-ingested file as fully ingested forever. See
     * finding #5 of the final-review fix wave for the full reasoning.
     */
    private static boolean hasNoKafkaSendFailures(IngestionResult result) {
        return result.errors().stream().noneMatch(error -> error.lineNumber() == -1);
    }

    private IngestionResult runIngestion(Path path, String fingerprint) {
        List<String> lines;
        String contentHash;
        try {
            lines = Files.readAllLines(path);
            contentHash = ContentHash.compute(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        ParseResult parseResult = parser.parseAll(lines);

        List<ParseError> sendFailures = new ArrayList<>();
        List<ParsedRecord> records = parseResult.records();
        int published = 0;
        int consecutiveFailures = 0;
        int attempted = 0;
        for (ParsedRecord parsedRecord : records) {
            attempted++;
            FutureTransaction transaction = parsedRecord.transaction();
            String key = KafkaKeyBuilder.buildKey(transaction);
            String transactionId = TransactionIdBuilder.build(contentHash, parsedRecord.lineNumber());
            ProducerRecord<String, FutureTransaction> producerRecord = new ProducerRecord<>(
                    properties.topic(), null, key, transaction,
                    List.of(new RecordHeader("transactionId", transactionId.getBytes(StandardCharsets.UTF_8))));
            try {
                kafkaTemplate.send(producerRecord).get(10, TimeUnit.SECONDS);
                published++;
                consecutiveFailures = 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sendFailures.add(new ParseError(-1, key, "Kafka send interrupted: " + e.getMessage()));
                consecutiveFailures++;
            } catch (ExecutionException | TimeoutException e) {
                sendFailures.add(new ParseError(-1, key, "Kafka send failed: " + e.getMessage()));
                consecutiveFailures++;
            }
            if (consecutiveFailures >= MAX_CONSECUTIVE_SEND_FAILURES) {
                break;
            }
        }

        if (attempted < records.size()) {
            int notAttempted = records.size() - attempted;
            sendFailures.add(new ParseError(-1, "n/a",
                    "Aborted after " + MAX_CONSECUTIVE_SEND_FAILURES
                            + " consecutive Kafka send failures; " + notAttempted
                            + " record(s) not attempted"));
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

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl ingestion-service -am test -Dtest=IngestionServiceTest`
Expected: BUILD SUCCESS, all 9 tests pass.

- [ ] **Step 5: Run the full ingestion-service unit test suite (excluding the Testcontainers e2e test, which Task 5 updates)**

Run: `mvn -pl ingestion-service -am test -Dtest='!IngestionEndToEndTest'`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Commit**

```bash
git add ingestion-service/src/main/java/com/pfm/ingestion/IngestionService.java \
        ingestion-service/src/test/java/com/pfm/ingestion/IngestionServiceTest.java
git commit -m "feat(ingestion-service): attach content-derived transactionId as a Kafka header"
```

---

## Task 4: `ingestion-service` — end-to-end header wire-contract check

**Files:**
- Modify: `ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java`

**Interfaces:**
- Consumes: the header attached by Task 3's `IngestionService`.

Extends the existing Testcontainers e2e test with a structural check that every record consumed from the real topic carries a non-empty `transactionId` header that looks like a SHA-256 hex digest — deliberately not hardcoding the exact 717-line file's content hash (that would be a huge, unreadable literal and this is already covered precisely at the unit level in Task 2/3). This mirrors the existing test's own style: structural wire-contract checks on key/value, not brittle golden values.

- [ ] **Step 1: Add the header assertion to the existing test**

In `ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java`, add these imports:

```java
import org.apache.kafka.common.header.Header;
```

(alongside the existing `org.apache.kafka.clients.consumer.*` imports)

Then, inside `ingestingSampleFilePublishesAllRecordsAndIsIdempotentUntilForced()`, immediately after the existing `List<ConsumerRecord<String, String>> records = consumeAll(1434);` / `assertEquals(1434, records.size());` block (i.e. after part (a)'s key check and before or after part (b)'s value check — append as a new part (c) at the end of the test method, right before its closing brace):

```java

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
```

(`Pattern` is already imported at the top of the file via `java.util.regex.Pattern`.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl ingestion-service -am test -Dtest=IngestionEndToEndTest`
Expected: FAIL — `records.stream().allMatch(...)` returns `false` (or a `NullPointerException` on `header.value()`) because `IngestionService` doesn't attach the header yet in this test's context... but note Task 3 already implemented the header attachment. If Task 3 already landed, this step should actually PASS immediately — in that case, skip straight to Step 3's confirmation. (Requires Docker; see `ingestion-service/pom.xml`'s `testcontainers.docker.api.version` comment if "Could not find a valid Docker environment" appears.)

- [ ] **Step 3: Run the test to verify it passes**

Run: `mvn -pl ingestion-service -am test -Dtest=IngestionEndToEndTest`
Expected: BUILD SUCCESS.

- [ ] **Step 4: Commit**

```bash
git add ingestion-service/src/test/java/com/pfm/ingestion/IngestionEndToEndTest.java
git commit -m "test(ingestion-service): assert transactionId header reaches a real broker"
```

---

## Task 5: `processing-service` — scaffold the Spring Boot module

**Files:**
- Create: `processing-service/pom.xml`
- Modify: `pom.xml:20` (add `<module>processing-service</module>`)
- Create: `processing-service/src/main/java/com/pfm/processing/ProcessingServiceApplication.java`
- Create: `processing-service/src/main/java/com/pfm/processing/ProcessingProperties.java`
- Create: `processing-service/src/main/resources/application.yml`
- Create: `processing-service/README.md`
- Test: `processing-service/src/test/java/com/pfm/processing/ProcessingServiceApplicationTests.java`

**Interfaces:**
- Produces: `ProcessingProperties(String topic)` — a `@ConfigurationProperties(prefix = "processing")` record. Task 8 (`AggregationTopology`) consumes `properties.topic()`.

No new behavior yet — this is the scaffold, mirroring how `ingestion-service`'s scaffold task (commit `3520656`) established its module before any Kafka-specific code existed. `@EnableKafkaStreams` is deliberately NOT added here — that belongs on `AggregationTopology` (Task 8), keeping this task's context free of any live-broker requirement.

- [ ] **Step 1: Create `processing-service/pom.xml`**

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
  <artifactId>processing-service</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <properties>
    <java.version>21</java.version>

    <!-- See ingestion-service/pom.xml for the full explanation of why this exists:
         Testcontainers 1.21.3's Docker API fallback (1.32) is below what current
         Docker Desktop builds require, so it's pinned and independently overridable
         via -Dtestcontainers.docker.api.version=... per machine. -->
    <testcontainers.docker.api.version>1.51</testcontainers.docker.api.version>
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
      <!-- kafka-streams is an <optional>true</optional> dependency of spring-kafka,
           so it must be declared explicitly here. Version is managed by
           spring-boot-starter-parent to match the already-resolved kafka-clients:3.9.1. -->
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams</artifactId>
    </dependency>

    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-test</artifactId>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka-streams-test-utils</artifactId>
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
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <configuration>
          <systemPropertyVariables>
            <api.version>${testcontainers.docker.api.version}</api.version>
          </systemPropertyVariables>
        </configuration>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Register the module in the root `pom.xml`**

In `pom.xml`, change:

```xml
  <modules>
    <module>common</module>
    <module>ingestion-service</module>
  </modules>
```

to:

```xml
  <modules>
    <module>common</module>
    <module>ingestion-service</module>
    <module>processing-service</module>
  </modules>
```

- [ ] **Step 3: Create `ProcessingProperties`**

```java
package com.pfm.processing;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "processing")
public record ProcessingProperties(String topic) {
}
```

- [ ] **Step 4: Create `ProcessingServiceApplication`**

```java
package com.pfm.processing;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ProcessingServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProcessingServiceApplication.class, args);
    }
}
```

- [ ] **Step 5: Create `application.yml`**

`processing-service/src/main/resources/application.yml`:

```yaml
server:
  port: 8082

spring:
  application:
    name: processing-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    streams:
      application-id: processing-service

processing:
  topic: future-transactions
```

- [ ] **Step 6: Create the context-loads test**

`processing-service/src/test/java/com/pfm/processing/ProcessingServiceApplicationTests.java`. At this point in the plan there is no Kafka Streams topology bean yet (that's Task 8), so a plain context load needs no special Kafka properties:

```java
package com.pfm.processing;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class ProcessingServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
```

- [ ] **Step 7: Create `processing-service/README.md`**

```markdown
# processing-service

Spring Boot / Kafka Streams service that consumes the `future-transactions` Kafka
topic, dedupes retried/re-published records by their `transactionId` header, and
maintains a running per-(Client_Information, Product_Information) net quantity
aggregate — exposed via:

- `GET /api/report` — JSON
- `GET /api/report/csv` — CSV download (`Output.csv` format)

Design decisions: [docs/superpowers/specs/2026-08-09-processing-service-design.md](../docs/superpowers/specs/2026-08-09-processing-service-design.md).

## Running locally

Run these from the **repo root**:

```bash
docker compose up -d
mvn -q -DskipTests install
mvn -pl processing-service spring-boot:run
```

In a separate terminal, once `ingestion-service` has published some records (see its
own README) and this service has been running long enough to consume them:

```bash
curl http://localhost:8082/api/report
curl http://localhost:8082/api/report/csv
```
```

- [ ] **Step 8: Verify the scaffold compiles and the context-loads test passes**

Run: `mvn -pl processing-service -am test`
Expected: BUILD SUCCESS.

- [ ] **Step 9: Commit**

```bash
git add processing-service/pom.xml pom.xml \
        processing-service/src/main/java/com/pfm/processing/ProcessingServiceApplication.java \
        processing-service/src/main/java/com/pfm/processing/ProcessingProperties.java \
        processing-service/src/main/resources/application.yml \
        processing-service/README.md \
        processing-service/src/test/java/com/pfm/processing/ProcessingServiceApplicationTests.java
git commit -m "feat(processing-service): scaffold Spring Boot module"
```

---

## Task 6: `processing-service` — `TransactionSerde`

**Files:**
- Create: `processing-service/src/main/java/com/pfm/processing/streams/TransactionSerde.java`
- Test: `processing-service/src/test/java/com/pfm/processing/streams/TransactionSerdeTest.java`

**Interfaces:**
- Produces: `TransactionSerde.instance()` → `Serde<FutureTransaction>`. Tasks 7, 8, and 9's topology code all consume this.

Mirrors `ingestion-service`'s `KafkaProducerConfig` Jackson setup exactly (`JavaTimeModule` registered, `WRITE_DATES_AS_TIMESTAMPS` disabled) so deserialization on the consumer side matches what `ingestion-service` actually publishes. Built from plain Kafka `Serdes.serdeFrom(Serializer, Deserializer)` composing Spring Kafka's `JsonSerializer`/`JsonDeserializer`, rather than Spring Kafka's `JsonSerde` wrapper — this keeps the target type and `useHeadersIfPresent=false` (ignore any `__TypeId__` header, always deserialize as `FutureTransaction`) fully explicit.

- [ ] **Step 1: Write the failing test**

```java
package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransactionSerdeTest {

    @Test
    void roundTripsAFutureTransactionThroughSerializeAndDeserialize() {
        Serde<FutureTransaction> serde = TransactionSerde.instance();
        FutureTransaction original = new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B',
                100L, 30L,
                BigDecimal.valueOf(60, 2), "USD", 'D',
                BigDecimal.valueOf(30, 2), "USD", 'D',
                BigDecimal.ZERO, "JPY", 'D',
                LocalDate.of(2010, 8, 20), "001238", "0", "688032",
                BigDecimal.valueOf(925, 5), "TRDR12", "OPP0001", 'O');

        byte[] bytes = serde.serializer().serialize("future-transactions", original);
        FutureTransaction roundTripped = serde.deserializer().deserialize("future-transactions", bytes);

        assertEquals(original, roundTripped);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl processing-service -am test -Dtest=TransactionSerdeTest`
Expected: BUILD FAILURE — `TransactionSerde` doesn't exist.

- [ ] **Step 3: Implement `TransactionSerde`**

```java
package com.pfm.processing.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class TransactionSerde {

    private TransactionSerde() {
    }

    public static Serde<FutureTransaction> instance() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JsonSerializer<FutureTransaction> serializer = new JsonSerializer<>(objectMapper);
        JsonDeserializer<FutureTransaction> deserializer =
                new JsonDeserializer<>(FutureTransaction.class, objectMapper, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl processing-service -am test -Dtest=TransactionSerdeTest`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/streams/TransactionSerde.java \
        processing-service/src/test/java/com/pfm/processing/streams/TransactionSerdeTest.java
git commit -m "feat(processing-service): add TransactionSerde matching ingestion-service's wire format"
```

---

## Task 7: `processing-service` — `DedupProcessor`

**Files:**
- Create: `processing-service/src/main/java/com/pfm/processing/streams/DedupProcessor.java`
- Test: `processing-service/src/test/java/com/pfm/processing/streams/DedupProcessorTest.java`

**Interfaces:**
- Produces: `DedupProcessor` (Kafka Streams Processor API `Processor<String, FutureTransaction, String, FutureTransaction>`), `DedupProcessor.STORE_NAME = "seen-transaction-ids"`, `DedupProcessor.TRANSACTION_ID_HEADER = "transactionId"`. Task 8's `AggregationTopology` wires this into the production topology via `KStream.process(DedupProcessor::new, DedupProcessor.STORE_NAME)`.
- Consumes: `TransactionSerde` (Task 6) only in the test, to build a minimal test topology.

Reads the `transactionId` header off each record; if already present in the `seen-transaction-ids` store, drops the record (no forward — this is the mechanism that makes a retried/re-published duplicate a no-op). If the header is missing (defensive — shouldn't happen given `ingestion-service` always attaches it), forwards anyway rather than silently dropping real data, logging a warning.

- [ ] **Step 1: Write the failing test**

```java
package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DedupProcessorTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, FutureTransaction> inputTopic;
    private TestOutputTopic<String, FutureTransaction> outputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DedupProcessor.STORE_NAME), Serdes.String(), Serdes.Long()));
        KStream<String, FutureTransaction> deduped = builder
                .stream("input-topic", Consumed.with(Serdes.String(), TransactionSerde.instance()))
                .process(DedupProcessor::new, DedupProcessor.STORE_NAME);
        deduped.to("output-topic", org.apache.kafka.streams.kstream.Produced.with(
                Serdes.String(), TransactionSerde.instance()));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dedup-processor-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);

        inputTopic = driver.createInputTopic("input-topic", Serdes.String().serializer(),
                TransactionSerde.instance().serializer());
        outputTopic = driver.createOutputTopic("output-topic", Serdes.String().deserializer(),
                TransactionSerde.instance().deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void firstOccurrenceOfATransactionIdIsForwarded() {
        FutureTransaction transaction = transaction(100, 30);

        pipeInput("key-1", transaction, "tx-1");

        List<FutureTransaction> forwarded = outputTopic.readValuesToList();
        assertEquals(1, forwarded.size());
        assertEquals(transaction, forwarded.get(0));
    }

    @Test
    void repeatedTransactionIdIsDroppedNotForwarded() {
        FutureTransaction transaction = transaction(100, 30);

        pipeInput("key-1", transaction, "tx-1");
        pipeInput("key-1", transaction, "tx-1");

        List<FutureTransaction> forwarded = outputTopic.readValuesToList();
        assertEquals(1, forwarded.size(), "the second delivery of the same transactionId must be dropped");
    }

    @Test
    void sameFieldValuesWithDifferentTransactionIdsAreBothForwarded() {
        FutureTransaction transaction = transaction(100, 30);

        pipeInput("key-1", transaction, "tx-1");
        pipeInput("key-1", transaction, "tx-2");

        List<FutureTransaction> forwarded = outputTopic.readValuesToList();
        assertEquals(2, forwarded.size(),
                "two distinct transactionIds must both be forwarded, even with identical field values");
    }

    private void pipeInput(String key, FutureTransaction transaction, String transactionId) {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("transactionId", transactionId.getBytes(StandardCharsets.UTF_8)));
        inputTopic.pipeInput(new TestRecord<>(key, transaction, headers, (Instant) null));
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl processing-service -am test -Dtest=DedupProcessorTest`
Expected: BUILD FAILURE — `DedupProcessor` doesn't exist.

- [ ] **Step 3: Implement `DedupProcessor`**

```java
package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class DedupProcessor implements Processor<String, FutureTransaction, String, FutureTransaction> {

    public static final String STORE_NAME = "seen-transaction-ids";
    public static final String TRANSACTION_ID_HEADER = "transactionId";

    private static final Logger log = LoggerFactory.getLogger(DedupProcessor.class);

    private ProcessorContext<String, FutureTransaction> context;
    private KeyValueStore<String, Long> seenTransactionIds;

    @Override
    public void init(ProcessorContext<String, FutureTransaction> context) {
        this.context = context;
        this.seenTransactionIds = context.getStateStore(STORE_NAME);
    }

    @Override
    public void process(Record<String, FutureTransaction> record) {
        String transactionId = extractTransactionId(record);
        if (transactionId == null) {
            log.warn("Record for key {} has no transactionId header; forwarding without dedup tracking", record.key());
            context.forward(record);
            return;
        }
        if (seenTransactionIds.get(transactionId) != null) {
            return;
        }
        seenTransactionIds.put(transactionId, context.currentSystemTimeMs());
        context.forward(record);
    }

    private String extractTransactionId(Record<String, FutureTransaction> record) {
        Header header = record.headers().lastHeader(TRANSACTION_ID_HEADER);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl processing-service -am test -Dtest=DedupProcessorTest`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/streams/DedupProcessor.java \
        processing-service/src/test/java/com/pfm/processing/streams/DedupProcessorTest.java
git commit -m "feat(processing-service): add DedupProcessor to drop already-seen transactionIds"
```

---

## Task 8: `processing-service` — `AggregationTopology`

**Files:**
- Create: `processing-service/src/main/java/com/pfm/processing/streams/AggregationTopology.java`
- Test: `processing-service/src/test/java/com/pfm/processing/streams/AggregationTopologyTest.java`

**Interfaces:**
- Consumes: `DedupProcessor` (Task 7), `TransactionSerde` (Task 6), `ProcessingProperties` (Task 5).
- Produces: `AggregationTopology.NET_QUANTITY_STORE = "net-quantity-store"` (a persistent `KTable<String, Long>` of running net quantity keyed by `Client_Information|Product_Information`) and the static `AggregationTopology.build(StreamsBuilder, String topic)` method, which is framework-agnostic (no Spring dependency) so it's directly testable via `TopologyTestDriver` without a Spring context. Task 10 (`ReportService`) consumes `NET_QUANTITY_STORE` by name via Interactive Queries.

Wires source → `DedupProcessor` → `groupByKey().aggregate(...)`. The `@Bean` method (production entry point, invoked by Spring during context refresh when `StreamsBuilder` is autowired) is a thin wrapper around `build`.

- [ ] **Step 1: Write the failing test**

```java
package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AggregationTopologyTest {

    private static final String TOPIC = "future-transactions";
    private static final String KEY = "CL432100020001|SGXFUNK20100910";

    private TopologyTestDriver driver;
    private TestInputTopic<String, FutureTransaction> inputTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        AggregationTopology.build(builder, TOPIC);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregation-topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        driver = new TopologyTestDriver(builder.build(), props);

        inputTopic = driver.createInputTopic(TOPIC, Serdes.String().serializer(), TransactionSerde.instance().serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void aggregatesNetQuantityPerKeyAcrossMultipleRecords() {
        pipeInput(KEY, transaction(100, 30), "tx-1"); // net +70
        pipeInput(KEY, transaction(50, 80), "tx-2");   // net -30

        KeyValueStore<String, Long> store = driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(40L, store.get(KEY));
    }

    @Test
    void duplicateTransactionIdDoesNotDoubleCountInTheAggregate() {
        FutureTransaction transaction = transaction(100, 30); // net +70

        pipeInput(KEY, transaction, "tx-1");
        pipeInput(KEY, transaction, "tx-1"); // retried/re-published duplicate

        KeyValueStore<String, Long> store = driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(70L, store.get(KEY));
    }

    @Test
    void twoLegitimateTransactionsWithIdenticalFieldValuesBothCount() {
        FutureTransaction transaction = transaction(100, 30); // net +70 each

        pipeInput(KEY, transaction, "tx-1");
        pipeInput(KEY, transaction, "tx-2"); // different transactionId: a real second trade

        KeyValueStore<String, Long> store = driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertEquals(140L, store.get(KEY));
    }

    @Test
    void aNeverSeenKeyHasNoEntryInTheStore() {
        KeyValueStore<String, Long> store = driver.getKeyValueStore(AggregationTopology.NET_QUANTITY_STORE);
        assertNull(store.get("no-such-key"));
    }

    private void pipeInput(String key, FutureTransaction transaction, String transactionId) {
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader("transactionId", transactionId.getBytes(StandardCharsets.UTF_8)));
        inputTopic.pipeInput(new TestRecord<>(key, transaction, headers, (Instant) null));
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl processing-service -am test -Dtest=AggregationTopologyTest`
Expected: BUILD FAILURE — `AggregationTopology` doesn't exist.

- [ ] **Step 3: Implement `AggregationTopology`**

```java
package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import com.pfm.processing.ProcessingProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class AggregationTopology {

    public static final String NET_QUANTITY_STORE = "net-quantity-store";

    @Bean
    public KStream<String, FutureTransaction> netQuantityStream(StreamsBuilder streamsBuilder,
                                                                  ProcessingProperties properties) {
        return build(streamsBuilder, properties.topic());
    }

    static KStream<String, FutureTransaction> build(StreamsBuilder streamsBuilder, String topic) {
        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DedupProcessor.STORE_NAME), Serdes.String(), Serdes.Long()));

        KStream<String, FutureTransaction> source = streamsBuilder.stream(
                topic, Consumed.with(Serdes.String(), TransactionSerde.instance()));

        KStream<String, FutureTransaction> deduped =
                source.process(DedupProcessor::new, DedupProcessor.STORE_NAME);

        deduped.groupByKey(Grouped.with(Serdes.String(), TransactionSerde.instance()))
                .aggregate(
                        () -> 0L,
                        (key, transaction, total) -> total + (transaction.quantityLong() - transaction.quantityShort()),
                        Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(NET_QUANTITY_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long()));

        return deduped;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl processing-service -am test -Dtest=AggregationTopologyTest`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Update the context-loads test to avoid needing a live broker**

Now that `@EnableKafkaStreams` + a `KStream` `@Bean` exist, plain `@SpringBootTest` would try to start a real Kafka Streams app against `localhost:9092` during context refresh. Replace `processing-service/src/test/java/com/pfm/processing/ProcessingServiceApplicationTests.java`:

```java
package com.pfm.processing;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "spring.kafka.streams.auto-startup=false")
class ProcessingServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
```

(`spring.kafka.streams.auto-startup` maps to `KafkaProperties.Streams#setAutoStartup`, which `KafkaStreamsAnnotationDrivenConfiguration.KafkaStreamsFactoryBeanConfigurer` forwards directly to `StreamsBuilderFactoryBean.setAutoStartup(...)` — confirmed by inspecting the compiled `spring-boot-autoconfigure:3.5.4` classes. Real Kafka Streams startup is exercised for real in Task 12's Testcontainers e2e test instead.)

- [ ] **Step 6: Run the full processing-service test suite so far**

Run: `mvn -pl processing-service -am test -Dtest='!*EndToEndTest,!*GoldenTest'`
Expected: BUILD SUCCESS. (Excludes the Testcontainers-based tests, which don't exist yet until Tasks 12–13.)

- [ ] **Step 7: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/streams/AggregationTopology.java \
        processing-service/src/test/java/com/pfm/processing/streams/AggregationTopologyTest.java \
        processing-service/src/test/java/com/pfm/processing/ProcessingServiceApplicationTests.java
git commit -m "feat(processing-service): wire dedup + net-quantity aggregation topology"
```

---

## Task 9: `processing-service` — `ReportEntry` and `ReportService`

**Files:**
- Create: `processing-service/src/main/java/com/pfm/processing/report/ReportEntry.java`
- Create: `processing-service/src/main/java/com/pfm/processing/report/StoreNotReadyException.java`
- Create: `processing-service/src/main/java/com/pfm/processing/report/ReportService.java`
- Test: `processing-service/src/test/java/com/pfm/processing/report/ReportServiceTest.java`

**Interfaces:**
- Consumes: `AggregationTopology.NET_QUANTITY_STORE` (Task 8) by name.
- Produces: `record ReportEntry(String clientInformation, String productInformation, long netQuantity)` (with `@JsonProperty` mapping to `Client_Information`/`Product_Information`/`Total_Transaction_Amount`), `ReportService.currentReport(): List<ReportEntry>` (throws `StoreNotReadyException` if the Kafka Streams app isn't `RUNNING`). Task 11 (`ReportController`) consumes `ReportService.currentReport()`.

Uses Interactive Queries via `StreamsBuilderFactoryBean.getKafkaStreams()` (the Spring-managed handle to the underlying `KafkaStreams` instance). Unit-tested with a mocked `StreamsBuilderFactoryBean`/`KafkaStreams`/`ReadOnlyKeyValueStore`, so no broker is needed here — the real Interactive Query path against a live broker is exercised in Task 12.

- [ ] **Step 1: Write the failing test**

```java
package com.pfm.processing.report;

import com.pfm.processing.streams.AggregationTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReportServiceTest {

    @Test
    @SuppressWarnings("unchecked")
    void returnsEntriesSortedByClientThenProductInformation() {
        ReadOnlyKeyValueStore<String, Long> store = mock(ReadOnlyKeyValueStore.class);
        when(store.all()).thenReturn(iteratorOver(List.of(
                KeyValue.pair("CL432100020001|SGXFUNK20100910", 46L),
                KeyValue.pair("CL123400030001|CMEFUNK.20100910", -215L),
                KeyValue.pair("CL123400030001|CMEFUN120100910", 285L),
                KeyValue.pair("CL123400020001|SGXFUNK20100910", -52L))));

        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);
        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);

        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        ReportService service = new ReportService(factoryBean);
        List<ReportEntry> report = service.currentReport();

        assertEquals(4, report.size());
        assertEquals(new ReportEntry("CL123400020001", "SGXFUNK20100910", -52L), report.get(0));
        assertEquals(new ReportEntry("CL123400030001", "CMEFUN120100910", 285L), report.get(1));
        assertEquals(new ReportEntry("CL123400030001", "CMEFUNK.20100910", -215L), report.get(2));
        assertEquals(new ReportEntry("CL432100020001", "SGXFUNK20100910", 46L), report.get(3));
    }

    @Test
    void throwsStoreNotReadyExceptionWhenKafkaStreamsIsNotRunning() {
        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.REBALANCING);

        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        ReportService service = new ReportService(factoryBean);

        assertThrows(StoreNotReadyException.class, service::currentReport);
    }

    @Test
    void throwsStoreNotReadyExceptionWhenKafkaStreamsHasNotStartedYet() {
        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(null);

        ReportService service = new ReportService(factoryBean);

        assertThrows(StoreNotReadyException.class, service::currentReport);
    }

    private static KeyValueIterator<String, Long> iteratorOver(List<KeyValue<String, Long>> entries) {
        Iterator<KeyValue<String, Long>> delegate = entries.iterator();
        return new KeyValueIterator<>() {
            @Override
            public void close() {
            }

            @Override
            public String peekNextKey() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public KeyValue<String, Long> next() {
                return delegate.next();
            }
        };
    }
}
```

Note: `AggregationTopology.NET_QUANTITY_STORE` isn't referenced directly by this test (the store name is an implementation detail `ReportService` uses internally) — the `import` is unused in this exact form; drop it if your IDE flags it, it's not required for the test to compile or pass.

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl processing-service -am test -Dtest=ReportServiceTest`
Expected: BUILD FAILURE — `ReportEntry`, `ReportService`, `StoreNotReadyException` don't exist.

- [ ] **Step 3: Implement `ReportEntry`**

```java
package com.pfm.processing.report;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ReportEntry(
        @JsonProperty("Client_Information") String clientInformation,
        @JsonProperty("Product_Information") String productInformation,
        @JsonProperty("Total_Transaction_Amount") long netQuantity
) {
}
```

- [ ] **Step 4: Implement `StoreNotReadyException`**

```java
package com.pfm.processing.report;

public class StoreNotReadyException extends RuntimeException {

    public StoreNotReadyException() {
        super("processing-service's aggregate store is not ready yet "
                + "(Kafka Streams is still starting or rebalancing)");
    }
}
```

- [ ] **Step 5: Implement `ReportService`**

```java
package com.pfm.processing.report;

import com.pfm.processing.streams.AggregationTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Service
public class ReportService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public ReportService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public List<ReportEntry> currentReport() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            throw new StoreNotReadyException();
        }

        ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        AggregationTopology.NET_QUANTITY_STORE, QueryableStoreTypes.keyValueStore()));

        List<ReportEntry> entries = new ArrayList<>();
        try (KeyValueIterator<String, Long> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Long> entry = iterator.next();
                entries.add(toReportEntry(entry.key, entry.value));
            }
        }
        entries.sort(Comparator.comparing(ReportEntry::clientInformation)
                .thenComparing(ReportEntry::productInformation));
        return entries;
    }

    private ReportEntry toReportEntry(String key, long netQuantity) {
        int separatorIndex = key.indexOf('|');
        String clientInformation = key.substring(0, separatorIndex);
        String productInformation = key.substring(separatorIndex + 1);
        return new ReportEntry(clientInformation, productInformation, netQuantity);
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `mvn -pl processing-service -am test -Dtest=ReportServiceTest`
Expected: BUILD SUCCESS.

- [ ] **Step 7: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/report/ReportEntry.java \
        processing-service/src/main/java/com/pfm/processing/report/StoreNotReadyException.java \
        processing-service/src/main/java/com/pfm/processing/report/ReportService.java \
        processing-service/src/test/java/com/pfm/processing/report/ReportServiceTest.java
git commit -m "feat(processing-service): add ReportService with sorted Interactive-Query access"
```

---

## Task 10: `processing-service` — `ReportController`

**Files:**
- Create: `processing-service/src/main/java/com/pfm/processing/report/ReportController.java`
- Test: `processing-service/src/test/java/com/pfm/processing/report/ReportControllerTest.java`

**Interfaces:**
- Consumes: `ReportService.currentReport()` (Task 9).
- Produces: `GET /api/report`, `GET /api/report/csv`.

`GET /api/report/csv` builds the CSV body by hand (header row + one row per entry, plain `long`, no thousands separators), matching `sample-output/Output.csv`'s exact column names.

- [ ] **Step 1: Write the failing test**

```java
package com.pfm.processing.report;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ReportController.class)
class ReportControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    ReportService reportService;

    @Test
    void getReportReturnsJsonWithSpecFieldNames() throws Exception {
        when(reportService.currentReport()).thenReturn(List.of(
                new ReportEntry("CL432100020001", "SGXFUNK20100910", 46L),
                new ReportEntry("CL432100030001", "CMEFUN120100910", -79L)));

        mockMvc.perform(get("/api/report"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].Client_Information").value("CL432100020001"))
                .andExpect(jsonPath("$[0].Product_Information").value("SGXFUNK20100910"))
                .andExpect(jsonPath("$[0].Total_Transaction_Amount").value(46))
                .andExpect(jsonPath("$[1].Client_Information").value("CL432100030001"))
                .andExpect(jsonPath("$[1].Total_Transaction_Amount").value(-79));
    }

    @Test
    void getReportReturnsEmptyArrayWhenNothingAggregatedYet() throws Exception {
        when(reportService.currentReport()).thenReturn(List.of());

        mockMvc.perform(get("/api/report"))
                .andExpect(status().isOk())
                .andExpect(content().json("[]"));
    }

    @Test
    void getReportCsvReturnsExactHeaderAndRows() throws Exception {
        when(reportService.currentReport()).thenReturn(List.of(
                new ReportEntry("CL123400020001", "SGXFUNK20100910", -52L),
                new ReportEntry("CL123400030001", "CMEFUN120100910", 285L),
                new ReportEntry("CL123400030001", "CMEFUNK.20100910", -215L),
                new ReportEntry("CL432100020001", "SGXFUNK20100910", 46L),
                new ReportEntry("CL432100030001", "CMEFUN120100910", -79L)));

        String expectedCsv = "Client_Information,Product_Information,Total_Transaction_Amount\n"
                + "CL123400020001,SGXFUNK20100910,-52\n"
                + "CL123400030001,CMEFUN120100910,285\n"
                + "CL123400030001,CMEFUNK.20100910,-215\n"
                + "CL432100020001,SGXFUNK20100910,46\n"
                + "CL432100030001,CMEFUN120100910,-79\n";

        mockMvc.perform(get("/api/report/csv"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("text/csv"))
                .andExpect(content().string(expectedCsv));
    }

    @Test
    void getReportReturns503WhenStoreIsNotReady() throws Exception {
        when(reportService.currentReport()).thenThrow(new StoreNotReadyException());

        mockMvc.perform(get("/api/report"))
                .andExpect(status().isServiceUnavailable());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -pl processing-service -am test -Dtest=ReportControllerTest`
Expected: BUILD FAILURE — `ReportController` doesn't exist.

- [ ] **Step 3: Implement `ReportController`**

```java
package com.pfm.processing.report;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class ReportController {

    private final ReportService reportService;

    public ReportController(ReportService reportService) {
        this.reportService = reportService;
    }

    @GetMapping("/api/report")
    public List<ReportEntry> report() {
        return reportService.currentReport();
    }

    @GetMapping(value = "/api/report/csv", produces = "text/csv")
    public ResponseEntity<String> reportCsv() {
        List<ReportEntry> entries = reportService.currentReport();
        StringBuilder csv = new StringBuilder("Client_Information,Product_Information,Total_Transaction_Amount\n");
        for (ReportEntry entry : entries) {
            csv.append(entry.clientInformation())
                    .append(',')
                    .append(entry.productInformation())
                    .append(',')
                    .append(entry.netQuantity())
                    .append('\n');
        }
        return ResponseEntity.ok()
                .contentType(MediaType.valueOf("text/csv"))
                .header("Content-Disposition", "attachment; filename=\"Output.csv\"")
                .body(csv.toString());
    }

    @ExceptionHandler(StoreNotReadyException.class)
    @ResponseStatus(HttpStatus.SERVICE_UNAVAILABLE)
    public Map<String, String> handleStoreNotReady(StoreNotReadyException e) {
        return Map.of("error", e.getMessage());
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -pl processing-service -am test -Dtest=ReportControllerTest`
Expected: BUILD SUCCESS.

- [ ] **Step 5: Commit**

```bash
git add processing-service/src/main/java/com/pfm/processing/report/ReportController.java \
        processing-service/src/test/java/com/pfm/processing/report/ReportControllerTest.java
git commit -m "feat(processing-service): add ReportController with JSON and CSV endpoints"
```

---

## Task 11: `processing-service` — Testcontainers end-to-end test

**Files:**
- Test: `processing-service/src/test/java/com/pfm/processing/ProcessingEndToEndTest.java`

**Interfaces:**
- Consumes: the full production wiring from Tasks 5–10 (`AggregationTopology`, `ReportController`, real Spring context).

Spins up a real Kafka broker (Testcontainers), a real `processing-service` Spring context against it, publishes hand-constructed messages (with `transactionId` headers built the same way `ingestion-service` builds them, but without depending on `ingestion-service` itself), and polls `GET /api/report` until the expected deduped/aggregated result appears — mirroring `IngestionEndToEndTest`'s own Testcontainers style.

- [ ] **Step 1: Write the test**

```java
package com.pfm.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pfm.common.domain.FutureTransaction;
import com.pfm.processing.report.ReportEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
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

    private static final String KEY = "CL432100020001|SGXFUNK20100910";

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.2");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.streams.application-id", () -> "processing-service-e2e-test");
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
        String[] parts = key.split("\\|", 2);
        long deadline = System.currentTimeMillis() + 30_000;
        ResponseEntity<ReportEntry[]> lastResponse = null;
        while (System.currentTimeMillis() < deadline) {
            ResponseEntity<ReportEntry[]> response = restTemplate.getForEntity("/api/report", ReportEntry[].class);
            lastResponse = response;
            if (response.getStatusCode().value() == 200 && response.getBody() != null) {
                for (ReportEntry entry : response.getBody()) {
                    if (entry.clientInformation().equals(parts[0])
                            && entry.productInformation().equals(parts[1])
                            && entry.netQuantity() == expectedNetQuantity) {
                        return;
                    }
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        fail("expected report entry for " + key + " with netQuantity=" + expectedNetQuantity
                + " not observed within 30s; last response body: "
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
```

- [ ] **Step 2: Run the test to verify it fails or passes for the right reasons**

Run: `mvn -pl processing-service -am test -Dtest=ProcessingEndToEndTest`
Expected: BUILD SUCCESS on the first run if Tasks 5–10 are all correctly implemented (this test doesn't introduce new production code, only exercises it) — treat any failure as a real bug to fix in the earlier tasks' code, not something to patch here. Requires Docker (see the `testcontainers.docker.api.version` note in `processing-service/pom.xml`).

- [ ] **Step 3: Commit**

```bash
git add processing-service/src/test/java/com/pfm/processing/ProcessingEndToEndTest.java
git commit -m "test(processing-service): add Testcontainers end-to-end dedup+aggregate test"
```

---

## Task 12: Full-pipeline golden test (`ingestion-service` → `processing-service`)

**Files:**
- Modify: `processing-service/pom.xml` (add a test-scope dependency on `ingestion-service`)
- Create: `processing-service/src/test/resources/Input.txt` (copy of `sample-data/Input.txt`)
- Test: `processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java`

**Interfaces:**
- Consumes: `IngestionServiceApplication` (real, from `ingestion-service`'s main sourceset) and `ProcessingServiceApplication` (real, from this module), both booted manually via `SpringApplicationBuilder` against a shared Testcontainers Kafka broker.

The strongest correctness signal available: drives the real `POST /api/ingest` against the real 717-record sample file, and asserts `processing-service`'s `GET /api/report/csv` matches `sample-output/Output.csv`'s values exactly (compared after this service's deterministic sort, since the sample file's own row order doesn't happen to already be sorted that way).

Verified independently (Python re-implementation of the same aggregation logic against `sample-data/Input.txt`, cross-checked against `sample-output/Output.csv`) — sorted by (Client_Information, Product_Information):

```
CL123400020001,SGXFUNK20100910,-52
CL123400030001,CMEFUN120100910,285
CL123400030001,CMEFUNK.20100910,-215
CL432100020001,SGXFUNK20100910,46
CL432100030001,CMEFUN120100910,-79
```

- [ ] **Step 1: Add the test-scope dependency on `ingestion-service`**

In `processing-service/pom.xml`, inside `<dependencies>`, add (right after the existing `com.pfm:common` dependency):

```xml
    <dependency>
      <!-- Test-only: FullPipelineGoldenTest boots a real ingestion-service context
           alongside this module's own, to validate the whole parse -> publish ->
           dedup -> aggregate -> report path against the real sample data. Never used
           by processing-service's production code. -->
      <groupId>com.pfm</groupId>
      <artifactId>ingestion-service</artifactId>
      <version>0.1.0-SNAPSHOT</version>
      <scope>test</scope>
    </dependency>
```

- [ ] **Step 2: Copy the sample file into test resources**

```bash
cp sample-data/Input.txt processing-service/src/test/resources/Input.txt
```

- [ ] **Step 3: Write the test**

```java
package com.pfm.processing;

import com.pfm.ingestion.IngestionResult;
import com.pfm.ingestion.IngestionServiceApplication;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
class FullPipelineGoldenTest {

    private static final String EXPECTED_CSV =
            "Client_Information,Product_Information,Total_Transaction_Amount\n"
                    + "CL123400020001,SGXFUNK20100910,-52\n"
                    + "CL123400030001,CMEFUN120100910,285\n"
                    + "CL123400030001,CMEFUNK.20100910,-215\n"
                    + "CL432100020001,SGXFUNK20100910,46\n"
                    + "CL432100030001,CMEFUN120100910,-79\n";

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.9.2");

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
    }

    @Test
    void fullPipelineProducesTheExpectedDailySummary() throws URISyntaxException {
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
                        "processing.topic=future-transactions")
                .run();

        RestTemplate rest = new RestTemplate();
        IngestionResult ingestResult = rest.postForObject(
                "http://localhost:18081/api/ingest", null, IngestionResult.class);
        assertEquals(717, ingestResult.published());

        String csv = awaitFullReportCsv(rest);
        assertEquals(EXPECTED_CSV, csv);
    }

    private String awaitFullReportCsv(RestTemplate rest) {
        long deadline = System.currentTimeMillis() + 60_000;
        String lastBody = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                lastBody = rest.getForObject("http://localhost:18082/api/report/csv", String.class);
                if (lastBody != null && lastBody.lines().count() == 6) {
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
        fail("processing-service did not produce the expected 6-line report within 60s; last body: " + lastBody);
        return null;
    }
}
```

- [ ] **Step 4: Run the test**

Run: `mvn -pl processing-service -am test -Dtest=FullPipelineGoldenTest`
Expected: BUILD SUCCESS. This is the slowest test in the suite (two full Spring Boot contexts + a real Kafka broker + polling); give it time before assuming it's hung. Requires Docker.

- [ ] **Step 5: Commit**

```bash
git add processing-service/pom.xml \
        processing-service/src/test/resources/Input.txt \
        processing-service/src/test/java/com/pfm/processing/FullPipelineGoldenTest.java
git commit -m "test(processing-service): add full-pipeline golden test against the real sample data"
```

---

## Task 13: Update root README status

**Files:**
- Modify: `README.md` (the `## Status` section)

**Interfaces:** none — documentation only.

- [ ] **Step 1: Update the status section**

In `README.md`, change:

```markdown
## Status

- `common` — done: fixed-width parser + domain model.
- `ingestion-service` — done: `POST /api/ingest` reads the file and publishes to Kafka
  (JSON, keyed by client+product, idempotent per file version). See its
  [README](ingestion-service/README.md) for usage.
- `processing-service`, `frontend`, `k8s` — not started.
```

to:

```markdown
## Status

- `common` — done: fixed-width parser + domain model.
- `ingestion-service` — done: `POST /api/ingest` reads the file and publishes to Kafka
  (JSON, keyed by client+product, idempotent per file version, each record carrying a
  content-derived `transactionId` header for downstream dedup). See its
  [README](ingestion-service/README.md) for usage.
- `processing-service` — done: Kafka Streams consumer dedupes on `transactionId` and
  maintains a running per-(client, product) net-quantity aggregate, exposed via
  `GET /api/report` and `GET /api/report/csv`. See its
  [README](processing-service/README.md) for usage.
- `frontend`, `k8s` — not started.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update root README status for processing-service"
```

---

## Final verification (run after all tasks are complete)

Run the entire multi-module build once, end to end:

```bash
mvn -pl common,ingestion-service,processing-service -am verify
```

Expected: BUILD SUCCESS across all three modules — `common` (existing tests + Task 1's updates), `ingestion-service` (existing tests + Tasks 2–4's new/updated tests), `processing-service` (all of Tasks 5–12's tests, including both Testcontainers tests). This is also the point to run `superpowers:requesting-code-review` per the project's standing workflow before finishing the branch.
