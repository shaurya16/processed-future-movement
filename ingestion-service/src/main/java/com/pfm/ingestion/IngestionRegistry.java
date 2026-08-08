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
