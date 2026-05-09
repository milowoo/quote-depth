package com.quote.depth.model;

/**
 * Immutable price level entry — one row in the depth book.
 */
public record DepthEntry(long price, long quantity) {

    /**
     * Returns true if this entry represents a removal (zero quantity).
     */
    public boolean isRemoval() {
        return quantity <= 0;
    }
}
