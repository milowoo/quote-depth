package com.quote.depth.service;

import com.quote.depth.cache.DepthCache;
import com.quote.depth.model.DepthEntry;
import com.quote.depth.model.DepthMessage;
import com.quote.depth.proto.DepthProto.PbDepthUpdate;
import com.quote.depth.proto.DepthProto.PbPriceLevel;
import com.quote.depth.websocket.DepthWebSocketHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Orchestrator for depth data processing.
 * <p>
 * Decodes protobuf depth updates, applies them to the per-symbol cache,
 * then broadcasts via WebSocket to subscribed clients.
 * <p>
 * All cache mutations for a given symbol are serialised under a
 * per-symbol lock so that new-subscriber catch-up snapshots are
 * guaranteed to reflect a consistent state (no partially-applied
 * increment).  The lock is a simple {@link #getLock(String)} Java
 * monitor — only operations on the <em>same</em> symbol contend.
 */
@Service
public class DepthService {

    private static final Logger log = LoggerFactory.getLogger(DepthService.class);

    private final ConcurrentHashMap<String, DepthCache> caches = new ConcurrentHashMap<>();

    /** Per-symbol monitors for atomic cache + subscription operations. */
    private final ConcurrentHashMap<String, Object> symbolLocks = new ConcurrentHashMap<>();

    private final DepthWebSocketHandler webSocketHandler;

    /** True once the first depth SNAPSHOT has been received from Kafka. */
    private final AtomicBoolean warm = new AtomicBoolean(false);

    @Value("${depth.snapshot.max-levels:100}")
    private int maxSnapshotLevels;

    public DepthService(DepthWebSocketHandler webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
    }

    /** Whether the service has received at least one depth snapshot. */
    public boolean isWarm() {
        return warm.get();
    }

    @PostConstruct
    void logWarmStatus() {
        log.info("DepthService started — waiting for first SNAPSHOT from Kafka before marking ready");
    }

    /** Exposed so {@code DepthWebSocketHandler} can synchronise subscription changes. */
    public Object getLock(String symbol) {
        return symbolLocks.computeIfAbsent(symbol, k -> new Object());
    }

    // ====================== Kafka consumer (producer thread) ======================

    /**
     * Process a depth update from Kafka.
     * <p>
     * The cache update is done under the per-symbol lock so that
     * concurrent catch-up snapshots see a consistent state.
     * Broadcasting happens <em>outside</em> the lock to keep I/O
     * from blocking other operations on the same symbol.
     */
    public void processUpdate(byte[] payload) {
        try {
            PbDepthUpdate pb = PbDepthUpdate.parseFrom(payload);
            String symbol = pb.getSymbolId();
            String type = pb.getType();
            long timestamp = pb.getTimestamp();
            long traceId = pb.getTraceId();

            List<DepthEntry> bids = convertPriceLevels(pb.getBidsList());
            List<DepthEntry> asks = convertPriceLevels(pb.getAsksList());

            DepthMessage message;
            synchronized (getLock(symbol)) {
                DepthCache cache = caches.computeIfAbsent(symbol, DepthCache::new);

                if ("SNAPSHOT".equalsIgnoreCase(type)) {
                    cache.applySnapshot(bids, asks, timestamp, traceId);
                    if (warm.compareAndSet(false, true)) {
                        log.info("First SNAPSHOT received for symbol={}, traceId={} — service is now ready",
                                symbol, traceId);
                    }
                } else {
                    cache.applyIncrement(bids, asks, timestamp, traceId);
                }

                message = new DepthMessage(
                        symbol, type,
                        toStringPairs(cache.getBids(maxSnapshotLevels)),
                        toStringPairs(cache.getAsks(maxSnapshotLevels)),
                        timestamp, traceId
                );
            }

            webSocketHandler.broadcast(message);

        } catch (Exception e) {
            log.error("Failed to process depth update", e);
        }
    }

    // ====================== WebSocket subscriber thread ======================

    /**
     * Take a consistent snapshot of the current depth state for a symbol
     * <strong>while holding the per-symbol lock</strong>.
     * <p>
     * The caller (the WebSocket handler) must have already acquired
     * {@link #getLock(String)} before calling this method, and should
     * also add the session to the subscription sets inside the same
     * critical section so that no subsequent {@link #processUpdate}
     * can interleave.
     */
    public DepthMessage snapshotWhileLocked(String symbol) {
        DepthCache cache = caches.get(symbol);
        if (cache == null) return null;
        return new DepthMessage(
                symbol, "SNAPSHOT",
                toStringPairs(cache.getBids(maxSnapshotLevels)),
                toStringPairs(cache.getAsks(maxSnapshotLevels)),
                cache.lastUpdateTime(),
                cache.lastSnapshotTraceId()
        );
    }

    /**
     * Get current depth for a symbol (for REST API or internal query).
     */
    public DepthMessage getDepth(String symbol) {
        DepthCache cache = caches.get(symbol);
        if (cache == null) return null;
        return new DepthMessage(
                symbol, "SNAPSHOT",
                toStringPairs(cache.getBids(maxSnapshotLevels)),
                toStringPairs(cache.getAsks(maxSnapshotLevels)),
                cache.lastUpdateTime()
        );
    }

    public int cachedSymbolCount() {
        return caches.size();
    }

    // ---- helpers ----

    private static List<DepthEntry> convertPriceLevels(List<PbPriceLevel> levels) {
        if (levels == null || levels.isEmpty()) {
            return List.of();
        }
        List<DepthEntry> result = new ArrayList<>(levels.size());
        for (PbPriceLevel l : levels) {
            long price = parseLong(l.getPrice());
            long quantity = parseLong(l.getQuantity());
            result.add(new DepthEntry(price, quantity));
        }
        return result;
    }

    private static long parseLong(String value) {
        if (value == null || value.isEmpty()) {
            return 0;
        }
        // Remove decimal point and parse as long (all values are scaled integers)
        String cleaned = value.replace(".", "");
        try {
            return Long.parseLong(cleaned);
        } catch (NumberFormatException e) {
            String intPart = value.contains(".") ? value.substring(0, value.indexOf('.')) : value;
            return Long.parseLong(intPart);
        }
    }

    private static List<String[]> toStringPairs(List<DepthEntry> entries) {
        if (entries.isEmpty()) {
            return List.of();
        }
        List<String[]> result = new ArrayList<>(entries.size());
        for (DepthEntry e : entries) {
            result.add(new String[]{String.valueOf(e.price()), String.valueOf(e.quantity())});
        }
        return result;
    }
}
