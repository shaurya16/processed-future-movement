package com.pfm.ingestion;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Component
public class IngestionRegistry {

    public record CacheOutcome(IngestionResult result, boolean cached) {
    }

    private final ConcurrentHashMap<String, IngestionResult> cache = new ConcurrentHashMap<>();

    public CacheOutcome getOrCompute(String fingerprint, Supplier<IngestionResult> computation) {
        return getOrCompute(fingerprint, computation, result -> true);
    }

    /**
     * Computes (or reuses a cached) result for the given fingerprint. A freshly computed
     * result for which {@code shouldCache} returns false is still handed back to this caller,
     * but is deliberately NOT stored — so the next non-forced call for the same fingerprint
     * recomputes (and can retry) instead of replaying a known-incomplete result forever.
     */
    public CacheOutcome getOrCompute(String fingerprint, Supplier<IngestionResult> computation,
                                      Predicate<IngestionResult> shouldCache) {
        AtomicBoolean computed = new AtomicBoolean(false);
        AtomicReference<IngestionResult> freshResult = new AtomicReference<>();
        IngestionResult cached = cache.computeIfAbsent(fingerprint, fp -> {
            computed.set(true);
            IngestionResult result = computation.get();
            freshResult.set(result);
            // Returning null from a computeIfAbsent mapping function stores nothing,
            // leaving the key absent so the next call recomputes.
            return shouldCache.test(result) ? result : null;
        });
        if (!computed.get()) {
            return new CacheOutcome(cached, true);
        }
        return new CacheOutcome(cached != null ? cached : freshResult.get(), false);
    }

    public IngestionResult forceCompute(String fingerprint, Supplier<IngestionResult> computation) {
        IngestionResult result = computation.get();
        cache.put(fingerprint, result);
        return result;
    }
}
