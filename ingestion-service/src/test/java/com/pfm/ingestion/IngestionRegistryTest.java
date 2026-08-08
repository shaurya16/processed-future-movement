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
