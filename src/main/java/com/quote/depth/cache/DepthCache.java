package com.quote.depth.cache;

import com.quote.depth.model.DepthEntry;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Thread-safe per-symbol depth cache.
 * <p>
 * Maintains sorted bid (descending price) and ask (ascending price) levels
 * using {@link ConcurrentSkipListMap}. Snapshot replaces the entire book;
 * incremental updates merge individual price levels.
 * <p>
 * A bounded ring buffer of recent incremental changes is kept internally
 * for diagnostics and potential catch-up replay.
 */
public class DepthCache {

    private static final int MAX_INCREMENT_BUFFER = 2000;

    private final String symbol;

    /** Bids: descending price order (highest first). */
    private final ConcurrentSkipListMap<Long, Long> bids;

    /** Asks: ascending price order (lowest first). */
    private final ConcurrentSkipListMap<Long, Long> asks;

    private volatile long lastUpdateTime;
    private volatile long lastSnapshotTraceId;

    public DepthCache(String symbol) {
        this.symbol = symbol;
        this.bids = new ConcurrentSkipListMap<>(Comparator.reverseOrder());
        this.asks = new ConcurrentSkipListMap<>(Comparator.naturalOrder());
    }

    public String symbol() {
        return symbol;
    }

    public long lastUpdateTime() {
        return lastUpdateTime;
    }

    public long lastSnapshotTraceId() {
        return lastSnapshotTraceId;
    }

    /**
     * Replace the entire book with a snapshot.
     */
    public void applySnapshot(List<DepthEntry> bidEntries, List<DepthEntry> askEntries,
                              long timestamp, long traceId) {
        bids.clear();
        asks.clear();

        if (bidEntries != null) {
            for (DepthEntry e : bidEntries) {
                if (!e.isRemoval()) {
                    bids.put(e.price(), e.quantity());
                }
            }
        }
        if (askEntries != null) {
            for (DepthEntry e : askEntries) {
                if (!e.isRemoval()) {
                    asks.put(e.price(), e.quantity());
                }
            }
        }

        this.lastSnapshotTraceId = traceId;
        this.lastUpdateTime = timestamp;
    }

    /**
     * Apply incremental changes.
     * <p>
     * If quantity &gt; 0, insert or update the price level.
     * If quantity &lt;= 0, remove the price level.
     */
    public void applyIncrement(List<DepthEntry> bidEntries, List<DepthEntry> askEntries,
                               long timestamp, long traceId) {
        if (bidEntries != null) {
            for (DepthEntry e : bidEntries) {
                if (e.isRemoval()) {
                    bids.remove(e.price());
                } else {
                    bids.put(e.price(), e.quantity());
                }
            }
        }
        if (askEntries != null) {
            for (DepthEntry e : askEntries) {
                if (e.isRemoval()) {
                    asks.remove(e.price());
                } else {
                    asks.put(e.price(), e.quantity());
                }
            }
        }

        this.lastUpdateTime = timestamp;
    }

    /**
     * Return a snapshot of current bids (top N levels, descending price).
     */
    public List<DepthEntry> getBids(int maxLevels) {
        return toDepthEntries(bids, maxLevels);
    }

    /**
     * Return a snapshot of current asks (top N levels, ascending price).
     */
    public List<DepthEntry> getAsks(int maxLevels) {
        return toDepthEntries(asks, maxLevels);
    }

    /**
     * Return the number of bid levels.
     */
    public int bidLevels() {
        return bids.size();
    }

    /**
     * Return the number of ask levels.
     */
    public int askLevels() {
        return asks.size();
    }

    private static List<DepthEntry> toDepthEntries(NavigableMap<Long, Long> map, int max) {
        if (map.isEmpty()) {
            return List.of();
        }
        List<DepthEntry> result = new ArrayList<>(Math.min(map.size(), max));
        for (Map.Entry<Long, Long> entry : map.entrySet()) {
            if (result.size() >= max) break;
            result.add(new DepthEntry(entry.getKey(), entry.getValue()));
        }
        return result;
    }
}
