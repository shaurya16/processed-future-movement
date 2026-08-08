# Common Fixed-Width Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `common` Maven module — an annotation-driven fixed-width parser and domain model for System A future-transaction records — test-first, using real lines from `sample-data/Input.txt` as fixtures.

**Architecture:** A generic reflection-based `FixedWidthRecordParser` extracts `@FixedWidthField`-annotated positional substrings into any all-`String` record type. `RawFutureTransaction` is that raw positional record for System A's 176-byte layout. `FutureTransactionFactory` converts it into the typed `FutureTransaction` domain record (signed quantities, scaled decimals, dates). `FutureTransactionParser` is a small facade composing the two, offering a strict `parse()` and a skip-and-collect `parseAll()`.

**Tech Stack:** Java 21, Maven (multi-module), JUnit 5.

## Global Constraints

- Java 21 (`maven.compiler.release=21`), per the Requirements Specification.
- `common` has zero Spring/Kafka/framework dependencies — plain Java + JUnit 5 (test scope) only.
- Package root: `com.pfm.common`.
- Every fixed-width offset must trace to a named constant in `FieldPositions`, sourced from [docs/file-spec.md](../../file-spec.md) — no inline magic numbers for positions elsewhere.
- Test fixtures use real lines copied verbatim from `sample-data/Input.txt` wherever a real example exists; synthetic/hand-built fixtures are used only for cases with no real example in the sample (documented per-test, e.g. negative quantity signs, `C` credit indicator, malformed data — none occur in the 717-line sample, confirmed by inspection).
- One commit per task, after that task's tests pass.

## Notes on deviation from the approved spec

The design spec ([docs/superpowers/specs/2026-08-09-common-fixed-width-parser-design.md](../specs/2026-08-09-common-fixed-width-parser-design.md)) assigns `parseAll` to `FixedWidthRecordParser`. This plan instead puts `parseAll`/`parse` on a new `FutureTransactionParser` facade (in the `domain` package) that composes `FixedWidthRecordParser` + `FutureTransactionFactory`. Rationale: `FixedWidthRecordParser` is meant to be generic and reusable for any future fixed-width record type (that's the whole justification for going reflection-based over hand-written positional code, per spec decision #3) — it must not know about `FutureTransactionFactory`, which is domain-specific. The externally observable behavior (strict `parse`, skip-and-collect `parseAll`, same error semantics) is unchanged from the spec.

---

### Task 1: Maven multi-module scaffold

**Files:**
- Create: `pom.xml` (repo root)
- Create: `common/pom.xml`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: a buildable, testable Maven module `common` (groupId `com.pfm`, artifactId `common`, packaging `jar`), reachable via `mvn -pl common -am <goal>` from the repo root. `common` depends on `org.junit.jupiter:junit-jupiter` (test scope only, version managed via `org.junit:junit-bom` in the root `pom.xml`).

- [ ] **Step 1: Create the root `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.pfm</groupId>
  <artifactId>processed-future-movement</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>pom</packaging>

  <properties>
    <maven.compiler.release>21</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <junit.version>5.10.2</junit.version>
  </properties>

  <modules>
    <module>common</module>
  </modules>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.junit</groupId>
        <artifactId>junit-bom</artifactId>
        <version>${junit.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <build>
    <pluginManagement>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-surefire-plugin</artifactId>
          <version>3.2.5</version>
        </plugin>
      </plugins>
    </pluginManagement>
  </build>
</project>
```

- [ ] **Step 2: Create `common/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>com.pfm</groupId>
    <artifactId>processed-future-movement</artifactId>
    <version>0.1.0-SNAPSHOT</version>
  </parent>

  <artifactId>common</artifactId>
  <packaging>jar</packaging>

  <dependencies>
    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>
</project>
```

- [ ] **Step 3: Verify the module builds with no sources yet**

Run: `mvn -q -pl common -am test`
Expected: exits 0 (`BUILD SUCCESS`; no source or test files exist yet, so nothing runs — this only proves the POM wiring is correct).

- [ ] **Step 4: Commit**

```bash
git add pom.xml common/pom.xml
git commit -m "build: scaffold Maven multi-module project with common module"
```

---

### Task 2: Generic fixed-width parsing engine

**Files:**
- Create: `common/src/main/java/com/pfm/common/fixedwidth/FixedWidthField.java`
- Create: `common/src/main/java/com/pfm/common/fixedwidth/FixedWidthParseException.java`
- Create: `common/src/main/java/com/pfm/common/fixedwidth/FixedWidthRecordParser.java`
- Test: `common/src/test/java/com/pfm/common/fixedwidth/FixedWidthRecordParserTest.java`

**Interfaces:**
- Consumes: the Maven module from Task 1.
- Produces:
  - `@FixedWidthField(int start, int length)` — annotation, `@Target(ElementType.RECORD_COMPONENT)`, `@Retention(RetentionPolicy.RUNTIME)`. `start` is 1-indexed inclusive (matches `docs/file-spec.md`'s own convention).
  - `FixedWidthParseException(int lineNumber, String rawLine, String reason)` — unchecked exception; accessors `int lineNumber()`, `String rawLine()`.
  - `FixedWidthRecordParser` — no-arg constructor; method `<T> T parse(String line, int lineNumber, Class<T> type)`. Requires every record component of `type` to be `String`-typed and carry `@FixedWidthField`; throws `FixedWidthParseException` if the line is too short for any field's declared offsets, if a component is missing the annotation, or if construction otherwise fails.

- [ ] **Step 1: Write the failing test**

Create `common/src/test/java/com/pfm/common/fixedwidth/FixedWidthRecordParserTest.java`:

```java
package com.pfm.common.fixedwidth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FixedWidthRecordParserTest {

    // Real line 1 from sample-data/Input.txt: client 4321, SGX/NK buy record.
    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    record SampleRecord(
        @FixedWidthField(start = 1, length = 3) String recordCode,
        @FixedWidthField(start = 4, length = 4) String clientType,
        @FixedWidthField(start = 8, length = 4) String clientNumber
    ) {}

    private final FixedWidthRecordParser parser = new FixedWidthRecordParser();

    @Test
    void extractsAnnotatedFieldsFromRealLine() {
        SampleRecord result = parser.parse(LINE_1, 1, SampleRecord.class);

        assertEquals("315", result.recordCode());
        assertEquals("CL", result.clientType());
        assertEquals("4321", result.clientNumber());
    }

    @Test
    void throwsWhenLineTooShortForField() {
        String truncated = "315CL";

        FixedWidthParseException exception = assertThrows(FixedWidthParseException.class,
            () -> parser.parse(truncated, 7, SampleRecord.class));

        assertEquals(7, exception.lineNumber());
        assertEquals(truncated, exception.rawLine());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl common -am test -Dtest=FixedWidthRecordParserTest`
Expected: compile error — `FixedWidthField`, `FixedWidthRecordParser`, `FixedWidthParseException` don't exist yet.

- [ ] **Step 3: Implement `FixedWidthField`**

Create `common/src/main/java/com/pfm/common/fixedwidth/FixedWidthField.java`:

```java
package com.pfm.common.fixedwidth;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as occupying a 1-indexed, inclusive-start position range
 * within a fixed-width line. {@code start} and {@code length} must match the field
 * position table in docs/file-spec.md.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.RECORD_COMPONENT)
public @interface FixedWidthField {
    int start();
    int length();
}
```

- [ ] **Step 4: Implement `FixedWidthParseException`**

Create `common/src/main/java/com/pfm/common/fixedwidth/FixedWidthParseException.java`:

```java
package com.pfm.common.fixedwidth;

public class FixedWidthParseException extends RuntimeException {

    private final int lineNumber;
    private final String rawLine;

    public FixedWidthParseException(int lineNumber, String rawLine, String reason) {
        super("Line " + lineNumber + ": " + reason);
        this.lineNumber = lineNumber;
        this.rawLine = rawLine;
    }

    public int lineNumber() {
        return lineNumber;
    }

    public String rawLine() {
        return rawLine;
    }
}
```

- [ ] **Step 5: Implement `FixedWidthRecordParser`**

Create `common/src/main/java/com/pfm/common/fixedwidth/FixedWidthRecordParser.java`:

```java
package com.pfm.common.fixedwidth;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;

/**
 * Generic parser that builds an all-String record type from a fixed-width line, using
 * each record component's {@link FixedWidthField} annotation to locate its substring.
 * Reusable for any record type meeting that shape, not just future-transaction records.
 */
public class FixedWidthRecordParser {

    public <T> T parse(String line, int lineNumber, Class<T> type) {
        RecordComponent[] components = type.getRecordComponents();
        Object[] args = new Object[components.length];
        Class<?>[] paramTypes = new Class<?>[components.length];

        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            FixedWidthField field = component.getAnnotation(FixedWidthField.class);
            if (field == null) {
                throw new FixedWidthParseException(lineNumber, line,
                        "Record component '" + component.getName() + "' has no @FixedWidthField");
            }

            int startIndex = field.start() - 1;
            int endIndex = startIndex + field.length();
            if (endIndex > line.length()) {
                throw new FixedWidthParseException(lineNumber, line,
                        "Field '" + component.getName() + "' needs " + endIndex
                                + " characters but line has " + line.length());
            }

            args[i] = line.substring(startIndex, endIndex).trim();
            paramTypes[i] = component.getType();
        }

        try {
            Constructor<T> constructor = type.getDeclaredConstructor(paramTypes);
            return constructor.newInstance(args);
        } catch (ReflectiveOperationException e) {
            throw new FixedWidthParseException(lineNumber, line,
                    "Failed to construct " + type.getSimpleName() + ": " + e.getMessage());
        }
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `mvn -q -pl common -am test -Dtest=FixedWidthRecordParserTest`
Expected: `BUILD SUCCESS`, both tests pass.

- [ ] **Step 7: Commit**

```bash
git add common/src/main/java/com/pfm/common/fixedwidth common/src/test/java/com/pfm/common/fixedwidth
git commit -m "feat(common): add generic annotation-driven fixed-width record parser"
```

---

### Task 3: `RawFutureTransaction` — full positional record

**Files:**
- Create: `common/src/main/java/com/pfm/common/domain/FieldPositions.java`
- Create: `common/src/main/java/com/pfm/common/domain/RawFutureTransaction.java`
- Test: `common/src/test/java/com/pfm/common/domain/RawFutureTransactionParsingTest.java`

**Interfaces:**
- Consumes: `FixedWidthField`, `FixedWidthRecordParser` from Task 2.
- Produces:
  - `FieldPositions` — 34 pairs of `public static final int <FIELD>_START` / `public static final int <FIELD>_LENGTH` constants, one pair per raw field, values sourced from `docs/file-spec.md`.
  - `RawFutureTransaction` — a record with 34 `String` components (see Step 3 for the exact list and order), each annotated `@FixedWidthField` using the `FieldPositions` constants.

- [ ] **Step 1: Write the failing test**

Create `common/src/test/java/com/pfm/common/domain/RawFutureTransactionParsingTest.java`:

```java
package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthRecordParser;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RawFutureTransactionParsingTest {

    // Real line 1: client 4321, account 0002, SGX/NK buy, quantityLong=1.
    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    // Real line 13: client 4321, account 0003, CME/N1 sell, quantityShort=3.
    private static final String LINE_13 =
        "315CL  432100030001FCC   FUCME N1    20100910JPY01S 0000000000 0000000003000000000000DUSD000000000015DUSD000000000000DJPY20100819059475      000308000093300000000             O";

    private final FixedWidthRecordParser parser = new FixedWidthRecordParser();

    @Test
    void parsesEveryFieldFromRealBuyRecord() {
        RawFutureTransaction raw = parser.parse(LINE_1, 1, RawFutureTransaction.class);

        assertEquals("315", raw.recordCode());
        assertEquals("CL", raw.clientType());
        assertEquals("4321", raw.clientNumber());
        assertEquals("0002", raw.accountNumber());
        assertEquals("0001", raw.subaccountNumber());
        assertEquals("SGXDC", raw.oppositePartyCode());
        assertEquals("FU", raw.productGroupCode());
        assertEquals("SGX", raw.exchangeCode());
        assertEquals("NK", raw.symbol());
        assertEquals("20100910", raw.expirationDateRaw());
        assertEquals("JPY", raw.currencyCode());
        assertEquals("01", raw.movementCode());
        assertEquals("B", raw.buySellCode());
        assertEquals("", raw.quantityLongSign());
        assertEquals("0000000001", raw.quantityLongRaw());
        assertEquals("", raw.quantityShortSign());
        assertEquals("0000000000", raw.quantityShortRaw());
        assertEquals("000000000060", raw.exchBrokerFeeRaw());
        assertEquals("D", raw.exchBrokerFeeDC());
        assertEquals("USD", raw.exchBrokerFeeCurrency());
        assertEquals("000000000030", raw.clearingFeeRaw());
        assertEquals("D", raw.clearingFeeDC());
        assertEquals("USD", raw.clearingFeeCurrency());
        assertEquals("000000000000", raw.commissionRaw());
        assertEquals("D", raw.commissionDC());
        assertEquals("JPY", raw.commissionCurrency());
        assertEquals("20100820", raw.transactionDateRaw());
        assertEquals("001238", raw.futureReference());
        assertEquals("0", raw.ticketNumber());
        assertEquals("688032", raw.externalNumber());
        assertEquals("000092500000000", raw.transactionPriceRaw());
        assertEquals("", raw.traderInitials());
        assertEquals("", raw.oppositeTraderId());
        assertEquals("O", raw.openCloseCode());
    }

    @Test
    void parsesEveryFieldFromRealSellRecord() {
        RawFutureTransaction raw = parser.parse(LINE_13, 13, RawFutureTransaction.class);

        assertEquals("4321", raw.clientNumber());
        assertEquals("0003", raw.accountNumber());
        assertEquals("FCC", raw.oppositePartyCode());
        assertEquals("CME", raw.exchangeCode());
        assertEquals("N1", raw.symbol());
        assertEquals("S", raw.buySellCode());
        assertEquals("", raw.quantityLongSign());
        assertEquals("0000000000", raw.quantityLongRaw());
        assertEquals("", raw.quantityShortSign());
        assertEquals("0000000003", raw.quantityShortRaw());
        assertEquals("000000000000", raw.exchBrokerFeeRaw());
        assertEquals("D", raw.exchBrokerFeeDC());
        assertEquals("USD", raw.exchBrokerFeeCurrency());
        assertEquals("000000000015", raw.clearingFeeRaw());
        assertEquals("D", raw.clearingFeeDC());
        assertEquals("USD", raw.clearingFeeCurrency());
        assertEquals("20100819", raw.transactionDateRaw());
        assertEquals("059475", raw.futureReference());
        assertEquals("", raw.ticketNumber());
        assertEquals("000308", raw.externalNumber());
        assertEquals("000093300000000", raw.transactionPriceRaw());
        assertEquals("O", raw.openCloseCode());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl common -am test -Dtest=RawFutureTransactionParsingTest`
Expected: compile error — `RawFutureTransaction` doesn't exist yet.

- [ ] **Step 3: Implement `FieldPositions`**

Create `common/src/main/java/com/pfm/common/domain/FieldPositions.java`:

```java
package com.pfm.common.domain;

/** Byte offsets for every field in the System A 176-byte future-transaction record, per docs/file-spec.md. */
public final class FieldPositions {

    public static final int RECORD_CODE_START = 1;
    public static final int RECORD_CODE_LENGTH = 3;

    public static final int CLIENT_TYPE_START = 4;
    public static final int CLIENT_TYPE_LENGTH = 4;

    public static final int CLIENT_NUMBER_START = 8;
    public static final int CLIENT_NUMBER_LENGTH = 4;

    public static final int ACCOUNT_NUMBER_START = 12;
    public static final int ACCOUNT_NUMBER_LENGTH = 4;

    public static final int SUBACCOUNT_NUMBER_START = 16;
    public static final int SUBACCOUNT_NUMBER_LENGTH = 4;

    public static final int OPPOSITE_PARTY_CODE_START = 20;
    public static final int OPPOSITE_PARTY_CODE_LENGTH = 6;

    public static final int PRODUCT_GROUP_CODE_START = 26;
    public static final int PRODUCT_GROUP_CODE_LENGTH = 2;

    public static final int EXCHANGE_CODE_START = 28;
    public static final int EXCHANGE_CODE_LENGTH = 4;

    public static final int SYMBOL_START = 32;
    public static final int SYMBOL_LENGTH = 6;

    public static final int EXPIRATION_DATE_START = 38;
    public static final int EXPIRATION_DATE_LENGTH = 8;

    public static final int CURRENCY_CODE_START = 46;
    public static final int CURRENCY_CODE_LENGTH = 3;

    public static final int MOVEMENT_CODE_START = 49;
    public static final int MOVEMENT_CODE_LENGTH = 2;

    public static final int BUY_SELL_CODE_START = 51;
    public static final int BUY_SELL_CODE_LENGTH = 1;

    public static final int QUANTITY_LONG_SIGN_START = 52;
    public static final int QUANTITY_LONG_SIGN_LENGTH = 1;

    public static final int QUANTITY_LONG_START = 53;
    public static final int QUANTITY_LONG_LENGTH = 10;

    public static final int QUANTITY_SHORT_SIGN_START = 63;
    public static final int QUANTITY_SHORT_SIGN_LENGTH = 1;

    public static final int QUANTITY_SHORT_START = 64;
    public static final int QUANTITY_SHORT_LENGTH = 10;

    public static final int EXCH_BROKER_FEE_START = 74;
    public static final int EXCH_BROKER_FEE_LENGTH = 12;

    public static final int EXCH_BROKER_FEE_DC_START = 86;
    public static final int EXCH_BROKER_FEE_DC_LENGTH = 1;

    public static final int EXCH_BROKER_FEE_CURRENCY_START = 87;
    public static final int EXCH_BROKER_FEE_CURRENCY_LENGTH = 3;

    public static final int CLEARING_FEE_START = 90;
    public static final int CLEARING_FEE_LENGTH = 12;

    public static final int CLEARING_FEE_DC_START = 102;
    public static final int CLEARING_FEE_DC_LENGTH = 1;

    public static final int CLEARING_FEE_CURRENCY_START = 103;
    public static final int CLEARING_FEE_CURRENCY_LENGTH = 3;

    public static final int COMMISSION_START = 106;
    public static final int COMMISSION_LENGTH = 12;

    public static final int COMMISSION_DC_START = 118;
    public static final int COMMISSION_DC_LENGTH = 1;

    public static final int COMMISSION_CURRENCY_START = 119;
    public static final int COMMISSION_CURRENCY_LENGTH = 3;

    public static final int TRANSACTION_DATE_START = 122;
    public static final int TRANSACTION_DATE_LENGTH = 8;

    public static final int FUTURE_REFERENCE_START = 130;
    public static final int FUTURE_REFERENCE_LENGTH = 6;

    public static final int TICKET_NUMBER_START = 136;
    public static final int TICKET_NUMBER_LENGTH = 6;

    public static final int EXTERNAL_NUMBER_START = 142;
    public static final int EXTERNAL_NUMBER_LENGTH = 6;

    public static final int TRANSACTION_PRICE_START = 148;
    public static final int TRANSACTION_PRICE_LENGTH = 15;

    public static final int TRADER_INITIALS_START = 163;
    public static final int TRADER_INITIALS_LENGTH = 6;

    public static final int OPPOSITE_TRADER_ID_START = 169;
    public static final int OPPOSITE_TRADER_ID_LENGTH = 7;

    public static final int OPEN_CLOSE_CODE_START = 176;
    public static final int OPEN_CLOSE_CODE_LENGTH = 1;

    private FieldPositions() {
    }
}
```

- [ ] **Step 4: Implement `RawFutureTransaction`**

Create `common/src/main/java/com/pfm/common/domain/RawFutureTransaction.java`:

```java
package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthField;

import static com.pfm.common.domain.FieldPositions.*;

/** Purely positional extraction of every field in a System A future-transaction record — no type conversion yet. */
public record RawFutureTransaction(
        @FixedWidthField(start = RECORD_CODE_START, length = RECORD_CODE_LENGTH) String recordCode,
        @FixedWidthField(start = CLIENT_TYPE_START, length = CLIENT_TYPE_LENGTH) String clientType,
        @FixedWidthField(start = CLIENT_NUMBER_START, length = CLIENT_NUMBER_LENGTH) String clientNumber,
        @FixedWidthField(start = ACCOUNT_NUMBER_START, length = ACCOUNT_NUMBER_LENGTH) String accountNumber,
        @FixedWidthField(start = SUBACCOUNT_NUMBER_START, length = SUBACCOUNT_NUMBER_LENGTH) String subaccountNumber,
        @FixedWidthField(start = OPPOSITE_PARTY_CODE_START, length = OPPOSITE_PARTY_CODE_LENGTH) String oppositePartyCode,
        @FixedWidthField(start = PRODUCT_GROUP_CODE_START, length = PRODUCT_GROUP_CODE_LENGTH) String productGroupCode,
        @FixedWidthField(start = EXCHANGE_CODE_START, length = EXCHANGE_CODE_LENGTH) String exchangeCode,
        @FixedWidthField(start = SYMBOL_START, length = SYMBOL_LENGTH) String symbol,
        @FixedWidthField(start = EXPIRATION_DATE_START, length = EXPIRATION_DATE_LENGTH) String expirationDateRaw,
        @FixedWidthField(start = CURRENCY_CODE_START, length = CURRENCY_CODE_LENGTH) String currencyCode,
        @FixedWidthField(start = MOVEMENT_CODE_START, length = MOVEMENT_CODE_LENGTH) String movementCode,
        @FixedWidthField(start = BUY_SELL_CODE_START, length = BUY_SELL_CODE_LENGTH) String buySellCode,
        @FixedWidthField(start = QUANTITY_LONG_SIGN_START, length = QUANTITY_LONG_SIGN_LENGTH) String quantityLongSign,
        @FixedWidthField(start = QUANTITY_LONG_START, length = QUANTITY_LONG_LENGTH) String quantityLongRaw,
        @FixedWidthField(start = QUANTITY_SHORT_SIGN_START, length = QUANTITY_SHORT_SIGN_LENGTH) String quantityShortSign,
        @FixedWidthField(start = QUANTITY_SHORT_START, length = QUANTITY_SHORT_LENGTH) String quantityShortRaw,
        @FixedWidthField(start = EXCH_BROKER_FEE_START, length = EXCH_BROKER_FEE_LENGTH) String exchBrokerFeeRaw,
        @FixedWidthField(start = EXCH_BROKER_FEE_DC_START, length = EXCH_BROKER_FEE_DC_LENGTH) String exchBrokerFeeDC,
        @FixedWidthField(start = EXCH_BROKER_FEE_CURRENCY_START, length = EXCH_BROKER_FEE_CURRENCY_LENGTH) String exchBrokerFeeCurrency,
        @FixedWidthField(start = CLEARING_FEE_START, length = CLEARING_FEE_LENGTH) String clearingFeeRaw,
        @FixedWidthField(start = CLEARING_FEE_DC_START, length = CLEARING_FEE_DC_LENGTH) String clearingFeeDC,
        @FixedWidthField(start = CLEARING_FEE_CURRENCY_START, length = CLEARING_FEE_CURRENCY_LENGTH) String clearingFeeCurrency,
        @FixedWidthField(start = COMMISSION_START, length = COMMISSION_LENGTH) String commissionRaw,
        @FixedWidthField(start = COMMISSION_DC_START, length = COMMISSION_DC_LENGTH) String commissionDC,
        @FixedWidthField(start = COMMISSION_CURRENCY_START, length = COMMISSION_CURRENCY_LENGTH) String commissionCurrency,
        @FixedWidthField(start = TRANSACTION_DATE_START, length = TRANSACTION_DATE_LENGTH) String transactionDateRaw,
        @FixedWidthField(start = FUTURE_REFERENCE_START, length = FUTURE_REFERENCE_LENGTH) String futureReference,
        @FixedWidthField(start = TICKET_NUMBER_START, length = TICKET_NUMBER_LENGTH) String ticketNumber,
        @FixedWidthField(start = EXTERNAL_NUMBER_START, length = EXTERNAL_NUMBER_LENGTH) String externalNumber,
        @FixedWidthField(start = TRANSACTION_PRICE_START, length = TRANSACTION_PRICE_LENGTH) String transactionPriceRaw,
        @FixedWidthField(start = TRADER_INITIALS_START, length = TRADER_INITIALS_LENGTH) String traderInitials,
        @FixedWidthField(start = OPPOSITE_TRADER_ID_START, length = OPPOSITE_TRADER_ID_LENGTH) String oppositeTraderId,
        @FixedWidthField(start = OPEN_CLOSE_CODE_START, length = OPEN_CLOSE_CODE_LENGTH) String openCloseCode
) {
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -q -pl common -am test -Dtest=RawFutureTransactionParsingTest`
Expected: `BUILD SUCCESS`, both tests pass.

- [ ] **Step 6: Commit**

```bash
git add common/src/main/java/com/pfm/common/domain/FieldPositions.java \
        common/src/main/java/com/pfm/common/domain/RawFutureTransaction.java \
        common/src/test/java/com/pfm/common/domain/RawFutureTransactionParsingTest.java
git commit -m "feat(common): add RawFutureTransaction positional record and FieldPositions"
```

---

### Task 4: `FutureTransaction` domain record + `FutureTransactionFactory`

**Files:**
- Create: `common/src/main/java/com/pfm/common/domain/FutureTransaction.java`
- Create: `common/src/main/java/com/pfm/common/domain/FutureTransactionFactory.java`
- Test: `common/src/test/java/com/pfm/common/domain/FutureTransactionFactoryTest.java`

**Interfaces:**
- Consumes: `RawFutureTransaction` (Task 3), `FixedWidthRecordParser` + `FixedWidthParseException` (Task 2).
- Produces:
  - `FutureTransaction` — 29-component record (types: `String`, `LocalDate`, `char`, `long`, `BigDecimal` as listed in Step 3).
  - `FutureTransactionFactory` — method `FutureTransaction from(RawFutureTransaction raw, int lineNumber)`. Throws `FixedWidthParseException` if a quantity/decimal field isn't numeric, a date isn't valid `CCYYMMDD`, or a single-character code field isn't exactly one character.

Sign/decimal conventions applied here (from the design spec): quantity sign blank or `+` = positive, `-` = negative. Money fields: raw digits scaled by the field's implied decimal count (fees/commission = 2, transaction price = 7 — price has no D/C field, so it's never negated); D/C indicator `D` negates the scaled amount, `C` leaves it positive (flagged assumption — no `C` example exists in the sample data).

- [ ] **Step 1: Write the failing test (real-data cases)**

Create `common/src/test/java/com/pfm/common/domain/FutureTransactionFactoryTest.java`:

```java
package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthParseException;
import com.pfm.common.fixedwidth.FixedWidthRecordParser;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FutureTransactionFactoryTest {

    private static final String LINE_1 =
        "315CL  432100020001SGXDC FUSGX NK    20100910JPY01B 0000000001 0000000000000000000060DUSD000000000030DUSD000000000000DJPY201008200012380     688032000092500000000             O";

    private static final String LINE_13 =
        "315CL  432100030001FCC   FUCME N1    20100910JPY01S 0000000000 0000000003000000000000DUSD000000000015DUSD000000000000DJPY20100819059475      000308000093300000000             O";

    private final FixedWidthRecordParser recordParser = new FixedWidthRecordParser();
    private final FutureTransactionFactory factory = new FutureTransactionFactory();

    @Test
    void convertsRealBuyRecord() {
        RawFutureTransaction raw = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);

        FutureTransaction result = factory.from(raw, 1);

        assertEquals("4321", result.clientNumber());
        assertEquals("0002", result.accountNumber());
        assertEquals("SGX", result.exchangeCode());
        assertEquals("FU", result.productGroupCode());
        assertEquals("NK", result.symbol());
        assertEquals(LocalDate.of(2010, 9, 10), result.expirationDate());
        assertEquals('B', result.buySellCode());
        assertEquals(1L, result.quantityLong());
        assertEquals(0L, result.quantityShort());
        assertEquals(new BigDecimal("-0.60"), result.exchBrokerFee());
        assertEquals(new BigDecimal("-0.30"), result.clearingFee());
        assertEquals(new BigDecimal("0.00"), result.commission());
        assertEquals(LocalDate.of(2010, 8, 20), result.transactionDate());
        assertEquals(new BigDecimal("9250.0000000"), result.transactionPrice());
        assertEquals('O', result.openCloseCode());
    }

    @Test
    void convertsRealSellRecord() {
        RawFutureTransaction raw = recordParser.parse(LINE_13, 13, RawFutureTransaction.class);

        FutureTransaction result = factory.from(raw, 13);

        assertEquals("0003", result.accountNumber());
        assertEquals("CME", result.exchangeCode());
        assertEquals("N1", result.symbol());
        assertEquals('S', result.buySellCode());
        assertEquals(0L, result.quantityLong());
        assertEquals(3L, result.quantityShort());
        assertEquals(new BigDecimal("0.00"), result.exchBrokerFee());
        assertEquals(new BigDecimal("-0.15"), result.clearingFee());
        assertEquals(LocalDate.of(2010, 8, 19), result.transactionDate());
        assertEquals(new BigDecimal("9330.0000000"), result.transactionPrice());
    }

    @Test
    void negativeQuantitySignProducesNegativeValue() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withNegativeLong = withQuantityLongSign(base, "-");

        FutureTransaction result = factory.from(withNegativeLong, 1);

        assertEquals(-1L, result.quantityLong());
    }

    @Test
    void plusQuantitySignProducesPositiveValue() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withPlusLong = withQuantityLongSign(base, "+");

        FutureTransaction result = factory.from(withPlusLong, 1);

        assertEquals(1L, result.quantityLong());
    }

    @Test
    void creditIndicatorProducesPositiveAmount() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withCredit = withExchBrokerFeeDC(base, "C");

        FutureTransaction result = factory.from(withCredit, 1);

        assertEquals(new BigDecimal("0.60"), result.exchBrokerFee());
    }

    @Test
    void nonNumericQuantityThrows() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withBadQuantity = withQuantityLongRaw(base, "AAAAAAAAAA");

        FixedWidthParseException exception = assertThrows(FixedWidthParseException.class,
            () -> factory.from(withBadQuantity, 1));

        assertEquals(1, exception.lineNumber());
    }

    @Test
    void invalidDateThrows() {
        RawFutureTransaction base = recordParser.parse(LINE_1, 1, RawFutureTransaction.class);
        RawFutureTransaction withBadDate = withExpirationDateRaw(base, "20109999");

        assertThrows(FixedWidthParseException.class, () -> factory.from(withBadDate, 1));
    }

    private RawFutureTransaction withQuantityLongSign(RawFutureTransaction base, String sign) {
        return new RawFutureTransaction(
            base.recordCode(), base.clientType(), base.clientNumber(), base.accountNumber(),
            base.subaccountNumber(), base.oppositePartyCode(), base.productGroupCode(),
            base.exchangeCode(), base.symbol(), base.expirationDateRaw(), base.currencyCode(),
            base.movementCode(), base.buySellCode(), sign, base.quantityLongRaw(),
            base.quantityShortSign(), base.quantityShortRaw(), base.exchBrokerFeeRaw(), base.exchBrokerFeeDC(),
            base.exchBrokerFeeCurrency(), base.clearingFeeRaw(), base.clearingFeeDC(), base.clearingFeeCurrency(),
            base.commissionRaw(), base.commissionDC(), base.commissionCurrency(), base.transactionDateRaw(),
            base.futureReference(), base.ticketNumber(), base.externalNumber(), base.transactionPriceRaw(),
            base.traderInitials(), base.oppositeTraderId(), base.openCloseCode()
        );
    }

    private RawFutureTransaction withQuantityLongRaw(RawFutureTransaction base, String rawDigits) {
        return new RawFutureTransaction(
            base.recordCode(), base.clientType(), base.clientNumber(), base.accountNumber(),
            base.subaccountNumber(), base.oppositePartyCode(), base.productGroupCode(),
            base.exchangeCode(), base.symbol(), base.expirationDateRaw(), base.currencyCode(),
            base.movementCode(), base.buySellCode(), base.quantityLongSign(), rawDigits,
            base.quantityShortSign(), base.quantityShortRaw(), base.exchBrokerFeeRaw(), base.exchBrokerFeeDC(),
            base.exchBrokerFeeCurrency(), base.clearingFeeRaw(), base.clearingFeeDC(), base.clearingFeeCurrency(),
            base.commissionRaw(), base.commissionDC(), base.commissionCurrency(), base.transactionDateRaw(),
            base.futureReference(), base.ticketNumber(), base.externalNumber(), base.transactionPriceRaw(),
            base.traderInitials(), base.oppositeTraderId(), base.openCloseCode()
        );
    }

    private RawFutureTransaction withExchBrokerFeeDC(RawFutureTransaction base, String dc) {
        return new RawFutureTransaction(
            base.recordCode(), base.clientType(), base.clientNumber(), base.accountNumber(),
            base.subaccountNumber(), base.oppositePartyCode(), base.productGroupCode(),
            base.exchangeCode(), base.symbol(), base.expirationDateRaw(), base.currencyCode(),
            base.movementCode(), base.buySellCode(), base.quantityLongSign(), base.quantityLongRaw(),
            base.quantityShortSign(), base.quantityShortRaw(), base.exchBrokerFeeRaw(), dc,
            base.exchBrokerFeeCurrency(), base.clearingFeeRaw(), base.clearingFeeDC(), base.clearingFeeCurrency(),
            base.commissionRaw(), base.commissionDC(), base.commissionCurrency(), base.transactionDateRaw(),
            base.futureReference(), base.ticketNumber(), base.externalNumber(), base.transactionPriceRaw(),
            base.traderInitials(), base.oppositeTraderId(), base.openCloseCode()
        );
    }

    private RawFutureTransaction withExpirationDateRaw(RawFutureTransaction base, String rawDate) {
        return new RawFutureTransaction(
            base.recordCode(), base.clientType(), base.clientNumber(), base.accountNumber(),
            base.subaccountNumber(), base.oppositePartyCode(), base.productGroupCode(),
            base.exchangeCode(), base.symbol(), rawDate, base.currencyCode(),
            base.movementCode(), base.buySellCode(), base.quantityLongSign(), base.quantityLongRaw(),
            base.quantityShortSign(), base.quantityShortRaw(), base.exchBrokerFeeRaw(), base.exchBrokerFeeDC(),
            base.exchBrokerFeeCurrency(), base.clearingFeeRaw(), base.clearingFeeDC(), base.clearingFeeCurrency(),
            base.commissionRaw(), base.commissionDC(), base.commissionCurrency(), base.transactionDateRaw(),
            base.futureReference(), base.ticketNumber(), base.externalNumber(), base.transactionPriceRaw(),
            base.traderInitials(), base.oppositeTraderId(), base.openCloseCode()
        );
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl common -am test -Dtest=FutureTransactionFactoryTest`
Expected: compile error — `FutureTransaction` and `FutureTransactionFactory` don't exist yet.

- [ ] **Step 3: Implement `FutureTransaction`**

Create `common/src/main/java/com/pfm/common/domain/FutureTransaction.java`:

```java
package com.pfm.common.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/** Fully typed future-transaction record: every raw field converted to its real type. */
public record FutureTransaction(
        String recordCode,
        String clientType,
        String clientNumber,
        String accountNumber,
        String subaccountNumber,
        String oppositePartyCode,
        String productGroupCode,
        String exchangeCode,
        String symbol,
        LocalDate expirationDate,
        String currencyCode,
        String movementCode,
        char buySellCode,
        long quantityLong,
        long quantityShort,
        BigDecimal exchBrokerFee,
        String exchBrokerFeeCurrency,
        BigDecimal clearingFee,
        String clearingFeeCurrency,
        BigDecimal commission,
        String commissionCurrency,
        LocalDate transactionDate,
        String futureReference,
        String ticketNumber,
        String externalNumber,
        BigDecimal transactionPrice,
        String traderInitials,
        String oppositeTraderId,
        char openCloseCode
) {
}
```

- [ ] **Step 4: Implement `FutureTransactionFactory`**

Create `common/src/main/java/com/pfm/common/domain/FutureTransactionFactory.java`:

```java
package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthParseException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/** Converts a purely positional {@link RawFutureTransaction} into a typed {@link FutureTransaction}. */
public class FutureTransactionFactory {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    public FutureTransaction from(RawFutureTransaction raw, int lineNumber) {
        return new FutureTransaction(
                raw.recordCode(),
                raw.clientType(),
                raw.clientNumber(),
                raw.accountNumber(),
                raw.subaccountNumber(),
                raw.oppositePartyCode(),
                raw.productGroupCode(),
                raw.exchangeCode(),
                raw.symbol(),
                parseDate(raw.expirationDateRaw(), "expirationDate", lineNumber, raw),
                raw.currencyCode(),
                raw.movementCode(),
                parseChar(raw.buySellCode(), "buySellCode", lineNumber, raw),
                signedLong(raw.quantityLongSign(), raw.quantityLongRaw(), "quantityLong", lineNumber, raw),
                signedLong(raw.quantityShortSign(), raw.quantityShortRaw(), "quantityShort", lineNumber, raw),
                scaledDecimal(raw.exchBrokerFeeRaw(), raw.exchBrokerFeeDC(), 2, "exchBrokerFee", lineNumber, raw),
                raw.exchBrokerFeeCurrency(),
                scaledDecimal(raw.clearingFeeRaw(), raw.clearingFeeDC(), 2, "clearingFee", lineNumber, raw),
                raw.clearingFeeCurrency(),
                scaledDecimal(raw.commissionRaw(), raw.commissionDC(), 2, "commission", lineNumber, raw),
                raw.commissionCurrency(),
                parseDate(raw.transactionDateRaw(), "transactionDate", lineNumber, raw),
                raw.futureReference(),
                raw.ticketNumber(),
                raw.externalNumber(),
                unscaledDecimal(raw.transactionPriceRaw(), 7, "transactionPrice", lineNumber, raw),
                raw.traderInitials(),
                raw.oppositeTraderId(),
                parseChar(raw.openCloseCode(), "openCloseCode", lineNumber, raw)
        );
    }

    private long signedLong(String sign, String rawValue, String fieldName, int lineNumber, RawFutureTransaction raw) {
        long value = parseLong(rawValue, fieldName, lineNumber, raw);
        return "-".equals(sign) ? -value : value;
    }

    private BigDecimal scaledDecimal(String rawValue, String debitCreditCode, int decimals, String fieldName,
                                      int lineNumber, RawFutureTransaction raw) {
        BigDecimal magnitude = unscaledDecimal(rawValue, decimals, fieldName, lineNumber, raw);
        return "D".equals(debitCreditCode) ? magnitude.negate() : magnitude;
    }

    private BigDecimal unscaledDecimal(String rawValue, int decimals, String fieldName, int lineNumber,
                                        RawFutureTransaction raw) {
        long digits = parseLong(rawValue, fieldName, lineNumber, raw);
        return BigDecimal.valueOf(digits, decimals);
    }

    private long parseLong(String rawValue, String fieldName, int lineNumber, RawFutureTransaction raw) {
        try {
            return Long.parseLong(rawValue);
        } catch (NumberFormatException e) {
            throw new FixedWidthParseException(lineNumber, raw.toString(),
                    "Field '" + fieldName + "' is not numeric: '" + rawValue + "'");
        }
    }

    private char parseChar(String rawValue, String fieldName, int lineNumber, RawFutureTransaction raw) {
        if (rawValue.length() != 1) {
            throw new FixedWidthParseException(lineNumber, raw.toString(),
                    "Field '" + fieldName + "' must be exactly one character: '" + rawValue + "'");
        }
        return rawValue.charAt(0);
    }

    private LocalDate parseDate(String rawValue, String fieldName, int lineNumber, RawFutureTransaction raw) {
        try {
            return LocalDate.parse(rawValue, DATE_FORMAT);
        } catch (DateTimeParseException e) {
            throw new FixedWidthParseException(lineNumber, raw.toString(),
                    "Field '" + fieldName + "' is not a valid CCYYMMDD date: '" + rawValue + "'");
        }
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `mvn -q -pl common -am test -Dtest=FutureTransactionFactoryTest`
Expected: `BUILD SUCCESS`, all 7 tests pass.

- [ ] **Step 6: Commit**

```bash
git add common/src/main/java/com/pfm/common/domain/FutureTransaction.java \
        common/src/main/java/com/pfm/common/domain/FutureTransactionFactory.java \
        common/src/test/java/com/pfm/common/domain/FutureTransactionFactoryTest.java
git commit -m "feat(common): add FutureTransaction domain record and conversion factory"
```

---

### Task 5: `FutureTransactionParser` — skip-and-collect batch parsing

**Files:**
- Create: `common/src/main/java/com/pfm/common/fixedwidth/ParseError.java`
- Create: `common/src/main/java/com/pfm/common/domain/ParseResult.java`
- Create: `common/src/main/java/com/pfm/common/domain/FutureTransactionParser.java`
- Test: `common/src/test/java/com/pfm/common/domain/FutureTransactionParserTest.java`

**Interfaces:**
- Consumes: `FixedWidthRecordParser`, `FixedWidthParseException` (Task 2), `RawFutureTransaction` (Task 3), `FutureTransaction`, `FutureTransactionFactory` (Task 4).
- Produces:
  - `ParseError(int lineNumber, String rawLine, String reason)`.
  - `ParseResult(List<FutureTransaction> records, List<ParseError> errors)`.
  - `FutureTransactionParser` — no-arg constructor; `FutureTransaction parse(String line, int lineNumber)` (strict, throws `FixedWidthParseException`); `ParseResult parseAll(List<String> lines)` (skip-and-collect; line numbers are 1-indexed by position in the input list; never throws).

- [ ] **Step 1: Write the failing test**

Create `common/src/test/java/com/pfm/common/domain/FutureTransactionParserTest.java`:

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
        assertEquals("0002", result.records().get(0).accountNumber());
        assertEquals("0003", result.records().get(1).accountNumber());
    }

    @Test
    void parseAllSkipsBadLineAndCollectsError() {
        ParseResult result = parser.parseAll(List.of(LINE_1, TRUNCATED_LINE));

        assertEquals(1, result.records().size());
        assertEquals("0002", result.records().get(0).accountNumber());
        assertEquals(1, result.errors().size());
        assertEquals(2, result.errors().get(0).lineNumber());
        assertEquals(TRUNCATED_LINE, result.errors().get(0).rawLine());
    }

    @Test
    void parseAllRecoversAfterABadLineAndKeepsParsingSubsequentGoodLines() {
        ParseResult result = parser.parseAll(List.of(LINE_1, TRUNCATED_LINE, LINE_13));

        assertEquals(2, result.records().size());
        assertEquals("0002", result.records().get(0).accountNumber());
        assertEquals("0003", result.records().get(1).accountNumber());
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

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q -pl common -am test -Dtest=FutureTransactionParserTest`
Expected: compile error — `ParseError`, `ParseResult`, `FutureTransactionParser` don't exist yet.

- [ ] **Step 3: Implement `ParseError`**

Create `common/src/main/java/com/pfm/common/fixedwidth/ParseError.java`:

```java
package com.pfm.common.fixedwidth;

/** One line that failed to parse, with enough context to build an error report. */
public record ParseError(int lineNumber, String rawLine, String reason) {
}
```

- [ ] **Step 4: Implement `ParseResult`**

Create `common/src/main/java/com/pfm/common/domain/ParseResult.java`:

```java
package com.pfm.common.domain;

import com.pfm.common.fixedwidth.ParseError;

import java.util.List;

public record ParseResult(List<FutureTransaction> records, List<ParseError> errors) {
}
```

- [ ] **Step 5: Implement `FutureTransactionParser`**

Create `common/src/main/java/com/pfm/common/domain/FutureTransactionParser.java`:

```java
package com.pfm.common.domain;

import com.pfm.common.fixedwidth.FixedWidthParseException;
import com.pfm.common.fixedwidth.FixedWidthRecordParser;
import com.pfm.common.fixedwidth.ParseError;

import java.util.ArrayList;
import java.util.List;

/**
 * Domain-specific facade composing {@link FixedWidthRecordParser} and
 * {@link FutureTransactionFactory}. {@link #parse} is strict (throws on a bad line);
 * {@link #parseAll} is resilient (skip-and-collect), which is what a file-reading
 * caller like ingestion-service is expected to use.
 */
public class FutureTransactionParser {

    private final FixedWidthRecordParser recordParser = new FixedWidthRecordParser();
    private final FutureTransactionFactory factory = new FutureTransactionFactory();

    public FutureTransaction parse(String line, int lineNumber) {
        RawFutureTransaction raw = recordParser.parse(line, lineNumber, RawFutureTransaction.class);
        return factory.from(raw, lineNumber);
    }

    public ParseResult parseAll(List<String> lines) {
        List<FutureTransaction> records = new ArrayList<>();
        List<ParseError> errors = new ArrayList<>();

        for (int i = 0; i < lines.size(); i++) {
            int lineNumber = i + 1;
            try {
                records.add(parse(lines.get(i), lineNumber));
            } catch (FixedWidthParseException e) {
                errors.add(new ParseError(e.lineNumber(), e.rawLine(), e.getMessage()));
            }
        }

        return new ParseResult(records, errors);
    }
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `mvn -q -pl common -am test -Dtest=FutureTransactionParserTest`
Expected: `BUILD SUCCESS`, all 5 tests pass.

- [ ] **Step 7: Commit**

```bash
git add common/src/main/java/com/pfm/common/fixedwidth/ParseError.java \
        common/src/main/java/com/pfm/common/domain/ParseResult.java \
        common/src/main/java/com/pfm/common/domain/FutureTransactionParser.java \
        common/src/test/java/com/pfm/common/domain/FutureTransactionParserTest.java
git commit -m "feat(common): add skip-and-collect batch parsing via FutureTransactionParser"
```

---

### Task 6: Golden test over the full sample file

**Files:**
- Create: `common/src/test/resources/Input.txt` (copy of `sample-data/Input.txt`)
- Test: `common/src/test/java/com/pfm/common/domain/GoldenSampleFileTest.java`

**Interfaces:**
- Consumes: `FutureTransactionParser.parseAll` (Task 5).
- Produces: no new production code — a regression guard. If a future edit to `FieldPositions` breaks byte alignment, this test fails immediately.

- [ ] **Step 1: Copy the sample file into test resources**

```bash
mkdir -p common/src/test/resources
cp sample-data/Input.txt common/src/test/resources/Input.txt
```

- [ ] **Step 2: Write the test**

Create `common/src/test/java/com/pfm/common/domain/GoldenSampleFileTest.java`:

```java
package com.pfm.common.domain;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GoldenSampleFileTest {

    @Test
    void parsesEntireSampleFileWithNoErrors() throws IOException {
        List<String> lines = readSampleLines();

        ParseResult result = new FutureTransactionParser().parseAll(lines);

        assertEquals(717, result.records().size());
        assertTrue(result.errors().isEmpty(), "unexpected parse errors: " + result.errors());
    }

    private List<String> readSampleLines() throws IOException {
        List<String> lines = new ArrayList<>();
        try (InputStream in = getClass().getResourceAsStream("/Input.txt");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.US_ASCII))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }
        return lines;
    }
}
```

- [ ] **Step 3: Run the test to verify it passes**

Run: `mvn -q -pl common -am test -Dtest=GoldenSampleFileTest`
Expected: `BUILD SUCCESS`, `records().size() == 717`, zero errors.

- [ ] **Step 4: Run the full module test suite**

Run: `mvn -q -pl common -am test`
Expected: `BUILD SUCCESS`, every test from Tasks 2–6 passes together.

- [ ] **Step 5: Commit**

```bash
git add common/src/test/resources/Input.txt common/src/test/java/com/pfm/common/domain/GoldenSampleFileTest.java
git commit -m "test(common): add golden regression test over the full 717-line sample file"
```
