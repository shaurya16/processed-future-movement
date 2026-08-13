package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

class IngestionRegistryTest {

    private static final Instant T1 = Instant.parse("2026-08-12T14:31:52Z");
    private static final Instant T2 = Instant.parse("2026-08-12T15:00:00Z");

    /** A Clock whose instant can be moved, for asserting what does and does not re-stamp. */
    private static final class MutableClock extends Clock {
        private Instant now;

        private MutableClock(Instant now) {
            this.now = now;
        }

        private void set(Instant now) {
            this.now = now;
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

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
    void forceComputeEvictsAGoodCachedResultWhenTheForcedOneIsUncacheable() {
        IngestionRegistry registry = new IngestionRegistry();
        AtomicInteger invocations = new AtomicInteger();
        Supplier<IngestionResult> supplier = () -> {
            int call = invocations.incrementAndGet();
            return new IngestionResult("fp", call, call, 0, List.of(), false);
        };

        // A good result is cached, then a forced run produces an uncacheable one.
        // The stale good entry must go too, or the next non-forced call serves a
        // result that the forced run has already superseded.
        registry.getOrCompute("fp", supplier);
        registry.forceCompute("fp", supplier, result -> false);
        IngestionRegistry.CacheOutcome next = registry.getOrCompute("fp", supplier);

        assertFalse(next.cached());
        assertEquals(3, invocations.get());
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

    @Test
    void lastIngestIsEmptyBeforeAnythingHasBeenComputed() {
        IngestionRegistry registry = new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC));

        assertTrue(registry.lastIngest().isEmpty());
    }

    @Test
    void computingAnIngestionRecordsItsTimestampAndResult() {
        IngestionRegistry registry = new IngestionRegistry(Clock.fixed(T1, ZoneOffset.UTC));
        IngestionResult result = new IngestionResult("fp", 717, 717, 0, List.of(), false);

        registry.getOrCompute("fp", () -> result);

        IngestionRegistry.LastIngest last = registry.lastIngest().orElseThrow();
        assertEquals(T1, last.at());
        assertSame(result, last.result());
    }

    @Test
    void aCacheHitDoesNotAdvanceTheLastIngestTimestamp() {
        // A cache hit republishes nothing to Kafka, so reporting it as a fresh
        // ingestion would tell the UI activity happened when none did.
        MutableClock clock = new MutableClock(T1);
        IngestionRegistry registry = new IngestionRegistry(clock);
        IngestionResult result = new IngestionResult("fp", 717, 717, 0, List.of(), false);

        registry.getOrCompute("fp", () -> result);
        clock.set(T2);
        IngestionRegistry.CacheOutcome second = registry.getOrCompute("fp", () -> result);

        assertTrue(second.cached());
        assertEquals(T1, registry.lastIngest().orElseThrow().at());
    }

    @Test
    void forceComputeAdvancesTheLastIngestTimestamp() {
        MutableClock clock = new MutableClock(T1);
        IngestionRegistry registry = new IngestionRegistry(clock);

        registry.getOrCompute("fp", () -> new IngestionResult("fp", 1, 1, 0, List.of(), false));
        clock.set(T2);
        registry.forceCompute("fp", () -> new IngestionResult("fp", 2, 2, 0, List.of(), false));

        assertEquals(T2, registry.lastIngest().orElseThrow().at());
        assertEquals(2, registry.lastIngest().orElseThrow().result().totalLines());
    }

    @Test
    void anUncachedResultStillCountsAsAnIngestionThatHappened() {
        // A batch with Kafka send failures is deliberately not cached, but records
        // WERE published, so it must still be reported as the last ingestion.
        MutableClock clock = new MutableClock(T1);
        IngestionRegistry registry = new IngestionRegistry(clock);
        IngestionResult withSendFailure = new IngestionResult("fp", 10, 4, 6,
                List.of(new ParseError(-1, "key", "Kafka send failed")), false);

        registry.getOrCompute("fp", () -> withSendFailure, result -> false);

        assertEquals(T1, registry.lastIngest().orElseThrow().at());
        assertEquals(4, registry.lastIngest().orElseThrow().result().published());
    }
}
