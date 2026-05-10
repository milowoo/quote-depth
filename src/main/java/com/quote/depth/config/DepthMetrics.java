package com.quote.depth.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Centralized Micrometer metrics for the depth service.
 * <p>
 * Exposes counters, timers, and gauges for monitoring update latency,
 * throughput, cache size, and subscriber activity. Metrics are tagged
 * by symbol for per-market breakdown.
 */
@Component
public class DepthMetrics {

    private final MeterRegistry registry;

    /** Symbol → (type → Counter) for depth updates. */
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Counter>> updateCounters = new ConcurrentHashMap<>();

    /** Symbol → Timer for end-to-end processing latency. */
    private final ConcurrentHashMap<String, Timer> updateTimers = new ConcurrentHashMap<>();

    public DepthMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * Record a depth update (counter + latency).
     */
    public void recordUpdate(String symbol, String type, long latencyNanos) {
        counter(symbol, type).increment();
        timer(symbol).record(latencyNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Register gauges that report current bid/ask level count per symbol.
     */
    public void registerCacheGauges(String symbol, Supplier<Number> bidLevels, Supplier<Number> askLevels) {
        Gauge.builder("depth.cache.bid.levels", bidLevels)
                .tag("symbol", symbol)
                .description("Current number of bid price levels in cache")
                .register(registry);
        Gauge.builder("depth.cache.ask.levels", askLevels)
                .tag("symbol", symbol)
                .description("Current number of ask price levels in cache")
                .register(registry);
    }

    /**
     * Register a gauge that reports current WebSocket subscriber count for a symbol.
     */
    public void registerSubscriberGauge(String symbol, Supplier<Number> count) {
        Gauge.builder("depth.subscribers", count)
                .tag("symbol", symbol)
                .description("Number of WebSocket sessions subscribed to this symbol")
                .register(registry);
    }

    /**
     * Report the total number of cached symbols.
     */
    public void reportCachedSymbolCount(Supplier<Number> count) {
        Gauge.builder("depth.symbols.cached", count)
                .description("Number of symbols with active depth cache")
                .register(registry);
    }

    // ---- internal helpers ----

    private Counter counter(String symbol, String type) {
        return updateCounters
                .computeIfAbsent(symbol, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(type, t -> Counter.builder("depth.updates")
                        .tag("symbol", symbol)
                        .tag("type", t)
                        .description("Total depth update messages processed")
                        .register(registry));
    }

    private Timer timer(String symbol) {
        return updateTimers.computeIfAbsent(symbol, s -> Timer.builder("depth.update.latency")
                .tag("symbol", s)
                .description("End-to-end processing latency per depth update")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry));
    }
}
