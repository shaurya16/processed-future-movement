package com.pfm.ingestion;

import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Component
public class IngestionRegistry {

    public record CacheOutcome(IngestionResult result, boolean cached) {
    }

    /** The most recent ingestion that actually ran, as opposed to one served from cache. */
    public record LastIngest(Instant at, IngestionResult result) {
    }

    private final ConcurrentHashMap<String, IngestionResult> cache = new ConcurrentHashMap<>();
    private final AtomicReference<LastIngest> lastIngest = new AtomicReference<>();
    private final Clock clock;

    public IngestionRegistry() {
        this(Clock.systemUTC());
    }

    /** Test seam: lets a test pin or advance the timestamp deterministically. */
    IngestionRegistry(Clock clock) {
        this.clock = clock;
    }

    public Optional<LastIngest> lastIngest() {
        return Optional.ofNullable(lastIngest.get());
    }

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
            // Records WERE published here even if the result is judged uncacheable,
            // so this counts as an ingestion that happened.
            lastIngest.set(new LastIngest(clock.instant(), result));
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
        return forceCompute(fingerprint, computation, result -> true);
    }

    /**
     * Recomputes unconditionally, ignoring any cached result. Honours {@code shouldCache}
     * exactly as {@link #getOrCompute} does — force is the documented recovery path, so it
     * is the most likely route into a partial ingestion, and caching one here would let the
     * next non-forced call replay it as if the file had fully landed. A result judged
     * uncacheable also evicts the previous entry it supersedes, which is now stale.
     */
    public IngestionResult forceCompute(String fingerprint, Supplier<IngestionResult> computation,
                                         Predicate<IngestionResult> shouldCache) {
        IngestionResult result = computation.get();
        lastIngest.set(new LastIngest(clock.instant(), result));
        if (shouldCache.test(result)) {
            cache.put(fingerprint, result);
        } else {
            cache.remove(fingerprint);
        }
        return result;
    }
}
