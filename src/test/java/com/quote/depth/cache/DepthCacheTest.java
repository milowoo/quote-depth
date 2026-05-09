package com.quote.depth.cache;

import com.quote.depth.model.DepthEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class DepthCacheTest {

    private DepthCache cache;

    @BeforeEach
    void setUp() {
        cache = new DepthCache("BTCUSDT");
    }

    @Test
    void snapshot_populatesBidsAndAsks() {
        List<DepthEntry> bids = List.of(
                new DepthEntry(10000L, 10),
                new DepthEntry(9900L, 20)
        );
        List<DepthEntry> asks = List.of(
                new DepthEntry(10100L, 15),
                new DepthEntry(10200L, 5)
        );
        cache.applySnapshot(bids, asks, 1000L, 500L);

        assertEquals("BTCUSDT", cache.symbol());
        assertEquals(2, cache.bidLevels());
        assertEquals(2, cache.askLevels());
        assertTrue(cache.lastUpdateTime() > 0);
    }

    @Test
    void snapshot_clearsPreviousData() {
        cache.applySnapshot(
                List.of(new DepthEntry(10000L, 10)),
                List.of(new DepthEntry(10100L, 15)),
                1000L, 100L
        );
        cache.applySnapshot(
                List.of(new DepthEntry(9900L, 20)),
                List.of(new DepthEntry(10200L, 5)),
                2000L, 200L
        );
        assertEquals(1, cache.bidLevels());
        assertEquals(9900L, cache.getBids(10).getFirst().price());
    }

    @Test
    void snapshot_recordsTraceId() {
        cache.applySnapshot(List.of(), List.of(), 1000L, 999L);
        assertEquals(999L, cache.lastSnapshotTraceId());
    }

    @Test
    void increment_addsNewLevels() {
        cache.applyIncrement(
                List.of(new DepthEntry(10000L, 10)),
                List.of(new DepthEntry(10100L, 15)),
                1000L, 100L
        );
        assertEquals(1, cache.bidLevels());
        assertEquals(1, cache.askLevels());
    }

    @Test
    void increment_updatesExistingLevel() {
        cache.applySnapshot(
                List.of(new DepthEntry(10000L, 10)),
                List.of(new DepthEntry(10100L, 15)),
                1000L, 100L
        );
        cache.applyIncrement(
                List.of(new DepthEntry(10000L, 25)),
                List.of(),
                2000L, 101L
        );
        assertEquals(25L, cache.getBids(10).getFirst().quantity());
    }

    @Test
    void increment_removesLevelWhenQtyZero() {
        cache.applySnapshot(
                List.of(new DepthEntry(10000L, 10), new DepthEntry(9900L, 5)),
                List.of(new DepthEntry(10100L, 15)),
                1000L, 100L
        );
        assertEquals(2, cache.bidLevels());

        cache.applyIncrement(
                List.of(new DepthEntry(10000L, 0)),
                List.of(),
                2000L, 101L
        );
        assertEquals(1, cache.bidLevels());
        assertEquals(9900L, cache.getBids(10).getFirst().price());
    }

    @Test
    void increment_removesLevelWhenQtyNegative() {
        cache.applySnapshot(
                List.of(new DepthEntry(10000L, 10)),
                List.of(),
                1000L, 100L
        );
        cache.applyIncrement(
                List.of(new DepthEntry(10000L, -1)),
                List.of(),
                2000L, 101L
        );
        assertEquals(0, cache.bidLevels());
    }

    @Test
    void getBids_returnsDescendingOrder() {
        cache.applySnapshot(
                List.of(
                        new DepthEntry(9900L, 5),
                        new DepthEntry(10000L, 10),
                        new DepthEntry(9800L, 3)
                ),
                List.of(),
                1000L, 100L
        );
        List<DepthEntry> bids = cache.getBids(10);
        assertEquals(3, bids.size());
        assertTrue(bids.get(0).price() > bids.get(1).price());
        assertTrue(bids.get(1).price() > bids.get(2).price());
        assertEquals(10000L, bids.get(0).price());
    }

    @Test
    void getAsks_returnsAscendingOrder() {
        cache.applySnapshot(
                List.of(),
                List.of(
                        new DepthEntry(10200L, 5),
                        new DepthEntry(10100L, 10),
                        new DepthEntry(10300L, 3)
                ),
                1000L, 100L
        );
        List<DepthEntry> asks = cache.getAsks(10);
        assertEquals(3, asks.size());
        assertTrue(asks.get(0).price() < asks.get(1).price());
        assertTrue(asks.get(1).price() < asks.get(2).price());
        assertEquals(10100L, asks.get(0).price());
    }

    @Test
    void getBids_respectsMaxLevels() {
        cache.applySnapshot(
                List.of(
                        new DepthEntry(10000L, 1),
                        new DepthEntry(9900L, 2),
                        new DepthEntry(9800L, 3),
                        new DepthEntry(9700L, 4),
                        new DepthEntry(9600L, 5)
                ),
                List.of(),
                1000L, 100L
        );
        assertEquals(3, cache.getBids(3).size());
        assertEquals(5, cache.getBids(10).size());
    }

    @Test
    void snapshot_ignoresZeroQuantityInSnapshot() {
        cache.applySnapshot(
                List.of(new DepthEntry(10000L, 0), new DepthEntry(9900L, 5)),
                List.of(new DepthEntry(10100L, 0)),
                1000L, 100L
        );
        assertEquals(1, cache.bidLevels());
        assertEquals(0, cache.askLevels());
    }

    @Test
    void emptyCache_returnsEmptyLists() {
        assertTrue(cache.getBids(10).isEmpty());
        assertTrue(cache.getAsks(10).isEmpty());
        assertEquals(0, cache.bidLevels());
        assertEquals(0, cache.askLevels());
    }

    @Test
    void snapshot_overwritesIncrements() {
        cache.applyIncrement(List.of(new DepthEntry(9900L, 5)), List.of(), 1000L, 100L);
        cache.applyIncrement(List.of(new DepthEntry(9800L, 3)), List.of(), 1001L, 101L);
        assertEquals(2, cache.bidLevels());

        // Snapshot wipes everything and resets
        cache.applySnapshot(
                List.of(new DepthEntry(10000L, 10)),
                List.of(),
                2000L, 200L
        );
        assertEquals(1, cache.bidLevels());
        assertEquals(200L, cache.lastSnapshotTraceId());
    }

    @Test
    void concurrentOperations_areThreadSafe() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 1000;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        cache.applySnapshot(List.of(), List.of(), 0, 100L);

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            Thread t = new Thread(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        long price = 10000L + threadId * 100 + j;
                        cache.applyIncrement(
                                List.of(new DepthEntry(price, (long) j)),
                                List.of(new DepthEntry(price + 100, (long) j)),
                                System.currentTimeMillis(),
                                200 + threadId * operationsPerThread + j
                        );
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
            t.start();
        }
        latch.await();

        assertEquals(0, errors.get(), "Concurrent operations should not throw exceptions");
        assertTrue(cache.bidLevels() > 0);
        assertTrue(cache.askLevels() > 0);
    }
}
