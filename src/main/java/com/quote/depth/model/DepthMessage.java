package com.quote.depth.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;

/**
 * WebSocket message sent to downstream clients.
 * <p>
 * Format:
 * <pre>
 * {
 *   "symbol": "BTCUSDT",
 *   "type": "SNAPSHOT",
 *   "bids": [["30000.00", "1.5"], ...],
 *   "asks": [["30001.00", "0.8"], ...],
 *   "timestamp": 1715234567890,
 *   "traceId": 10042
 * }
 * </pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DepthMessage {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String symbol;
    private String type;         // "SNAPSHOT" or "INCREMENTAL"
    private List<String[]> bids; // [[priceStr, qtyStr], ...]
    private List<String[]> asks;
    private long timestamp;
    private long traceId;
    private int bidLevels;
    private int askLevels;

    public DepthMessage() {}

    public DepthMessage(String symbol, String type, List<String[]> bids, List<String[]> asks,
                        long timestamp) {
        this(symbol, type, bids, asks, timestamp, 0);
    }

    public DepthMessage(String symbol, String type, List<String[]> bids, List<String[]> asks,
                        long timestamp, long traceId) {
        this.symbol = symbol;
        this.type = type;
        this.bids = bids;
        this.asks = asks;
        this.timestamp = timestamp;
        this.traceId = traceId;
        if (bids != null) this.bidLevels = bids.size();
        if (asks != null) this.askLevels = asks.size();
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public List<String[]> getBids() { return bids; }
    public void setBids(List<String[]> bids) { this.bids = bids; }

    public List<String[]> getAsks() { return asks; }
    public void setAsks(List<String[]> asks) { this.asks = asks; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public long getTraceId() { return traceId; }
    public void setTraceId(long traceId) { this.traceId = traceId; }

    public int getBidLevels() { return bidLevels; }
    public int getAskLevels() { return askLevels; }

    /**
     * Serialize this message to JSON bytes for WebSocket transmission.
     */
    public byte[] toJsonBytes() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize DepthMessage", e);
        }
    }

    public String toJsonString() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize DepthMessage", e);
        }
    }
}
