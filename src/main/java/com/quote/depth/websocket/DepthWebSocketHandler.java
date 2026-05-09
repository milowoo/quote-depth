package com.quote.depth.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quote.depth.model.DepthMessage;
import com.quote.depth.service.DepthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentHashMap.KeySetView;

/**
 * WebSocket handler for depth data push.
 * <p>
 * Clients subscribe to symbols by sending:
 * <pre>{"action": "subscribe", "symbol": "BTCUSDT"}</pre>
 * or unsubscribe with:
 * <pre>{"action": "unsubscribe", "symbol": "BTCUSDT"}</pre>
 * <p>
 * On subscribe, the handler sends a full depth snapshot followed by
 * any buffered incremental updates, ensuring every connection has a
 * complete depth picture from the start.
 */
@Component
public class DepthWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(DepthWebSocketHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DepthService depthService;

    /** symbol -> set of sessions subscribed to that symbol. */
    private final Map<String, KeySetView<WebSocketSession, Boolean>> subscriptions = new ConcurrentHashMap<>();

    /** session -> set of symbols it subscribed to (for cleanup on disconnect). */
    private final Map<WebSocketSession, Set<String>> sessionSymbols = new ConcurrentHashMap<>();

    /** Sessions currently receiving their initial catch-up data. */
    private final Set<WebSocketSession> catchingUp = ConcurrentHashMap.newKeySet();

    /** Messages buffered for sessions still in catch-up. */
    private final Map<WebSocketSession, ConcurrentLinkedDeque<DepthMessage>> catchupBuffers = new ConcurrentHashMap<>();

    public DepthWebSocketHandler(DepthService depthService) {
        this.depthService = depthService;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("WebSocket connected: sessionId={}, remote={}", session.getId(), session.getRemoteAddress());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            Map<String, String> msg = MAPPER.readValue(message.getPayload(), Map.class);
            String action = msg.get("action");
            String symbol = msg.get("symbol");

            if (symbol == null || symbol.isBlank()) {
                sendError(session, "missing symbol");
                return;
            }
            symbol = symbol.trim().toUpperCase();

            if ("subscribe".equalsIgnoreCase(action)) {
                subscribe(session, symbol);
            } else if ("unsubscribe".equalsIgnoreCase(action)) {
                unsubscribe(session, symbol);
            } else {
                sendError(session, "unknown action: " + action);
            }
        } catch (Exception e) {
            log.warn("Failed to handle WS message: sessionId={}, payload={}",
                    session.getId(), message.getPayload(), e);
            sendError(session, "invalid message format");
        }
    }

    private void subscribe(WebSocketSession session, String symbol) {
        if (isSubscribed(session, symbol)) {
            log.info("Already subscribed: sessionId={}, symbol={}", session.getId(), symbol);
            return;
        }

        // Atomically (under per-symbol lock):
        //   1. read a consistent snapshot from the cache
        //   2. mark session as catching-up
        //   3. add session to the subscription set
        // No processUpdate for this symbol can interleave, so the
        // snapshot is guaranteed to reflect the state BEFORE any
        // yet-to-be-broadcast increment.
        DepthMessage snapshot;
        synchronized (depthService.getLock(symbol)) {
            snapshot = depthService.snapshotWhileLocked(symbol);

            catchingUp.add(session);
            subscriptions.computeIfAbsent(symbol, k -> ConcurrentHashMap.newKeySet()).add(session);
            sessionSymbols.computeIfAbsent(session, k -> ConcurrentHashMap.newKeySet()).add(symbol);
        }

        // Snapshot and buffered-increment send happens OUTSIDE the lock
        // so that I/O cannot block other operations on the same symbol.
        if (snapshot != null) {
            sendToSession(session, snapshot);
            log.debug("Catch-up sent snapshot for {} to sessionId={}", symbol, session.getId());
        } else {
            log.debug("No cached depth for {}, sessionId={} will receive first snapshot live",
                    symbol, session.getId());
        }

        // End catch-up: any messages that arrived during step 1-3 or
        // during the snapshot send where buffered — flush them now.
        catchingUp.remove(session);
        flushBuffer(session);

        log.info("Subscribed: sessionId={}, symbol={}", session.getId(), symbol);
    }

    private boolean isSubscribed(WebSocketSession session, String symbol) {
        Set<String> symbols = sessionSymbols.get(session);
        return symbols != null && symbols.contains(symbol);
    }

    private void unsubscribe(WebSocketSession session, String symbol) {
        Set<WebSocketSession> sessions = subscriptions.get(symbol);
        if (sessions != null) {
            sessions.remove(session);
        }
        Set<String> symbols = sessionSymbols.get(session);
        if (symbols != null) {
            symbols.remove(symbol);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.info("WebSocket disconnected: sessionId={}, status={}", session.getId(), status);
        // Clean up catch-up state
        catchingUp.remove(session);
        catchupBuffers.remove(session);
        // Remove session from all subscriptions
        Set<String> symbols = sessionSymbols.remove(session);
        if (symbols != null) {
            for (String symbol : symbols) {
                Set<WebSocketSession> sessions = subscriptions.get(symbol);
                if (sessions != null) {
                    sessions.remove(session);
                }
            }
        }
    }

    /**
     * Broadcast a depth message to all sessions subscribed to the given symbol.
     * Sessions still in catch-up receive the message buffered instead,
     * guaranteeing ordered delivery.
     */
    public void broadcast(DepthMessage message) {
        Set<WebSocketSession> sessions = subscriptions.get(message.getSymbol());
        if (sessions == null || sessions.isEmpty()) {
            return;
        }

        for (WebSocketSession session : sessions) {
            if (!session.isOpen()) {
                continue;
            }
            if (catchingUp.contains(session)) {
                // Buffer until catch-up completes — avoids interleaving with snapshot
                catchupBuffers
                        .computeIfAbsent(session, k -> new ConcurrentLinkedDeque<>())
                        .addLast(message);
            } else {
                sendToSession(session, message);
            }
        }
    }

    // ---- internal helpers ----

    private void sendToSession(WebSocketSession session, DepthMessage message) {
        try {
            synchronized (session) {
                session.sendMessage(new TextMessage(message.toJsonBytes()));
            }
        } catch (IOException e) {
            log.warn("Failed to send depth to sessionId={}", session.getId(), e);
        }
    }

    /**
     * Replay all messages that were buffered while the session was catching up.
     */
    private void flushBuffer(WebSocketSession session) {
        ConcurrentLinkedDeque<DepthMessage> buffer = catchupBuffers.remove(session);
        if (buffer == null || buffer.isEmpty()) {
            return;
        }
        log.debug("Flushing {} buffered messages for sessionId={}", buffer.size(), session.getId());
        for (DepthMessage msg : buffer) {
            sendToSession(session, msg);
        }
    }

    private void sendError(WebSocketSession session, String error) {
        try {
            String msg = "{\"error\":\"" + error + "\"}";
            synchronized (session) {
                session.sendMessage(new TextMessage(msg));
            }
        } catch (IOException e) {
            log.warn("Failed to send error to sessionId={}", session.getId(), e);
        }
    }

    public int getSubscriberCount() {
        return sessionSymbols.size();
    }

    public int getSubscriptionCount(String symbol) {
        Set<WebSocketSession> sessions = subscriptions.get(symbol);
        return sessions != null ? sessions.size() : 0;
    }
}
