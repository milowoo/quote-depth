package com.quote.depth.config;

import com.quote.depth.websocket.DepthWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final DepthWebSocketHandler depthWebSocketHandler;

    public WebSocketConfig(DepthWebSocketHandler depthWebSocketHandler) {
        this.depthWebSocketHandler = depthWebSocketHandler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(depthWebSocketHandler, "/depth")
                .setAllowedOrigins("*");
    }

    @Bean
    public ServletServerContainerFactoryBean webSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(256 * 1024);
        container.setMaxBinaryMessageBufferSize(256 * 1024);
        container.setMaxSessionIdleTimeout(300_000L);
        return container;
    }
}
