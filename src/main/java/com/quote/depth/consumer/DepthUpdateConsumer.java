package com.quote.depth.consumer;

import com.quote.depth.service.DepthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for depth update messages from the matching engine.
 * <p>
 * Protobuf-serialized {@code PbDepthUpdate} messages arrive on the depth topic.
 */
@Component
public class DepthUpdateConsumer {

    private static final Logger log = LoggerFactory.getLogger(DepthUpdateConsumer.class);

    private final DepthService depthService;

    public DepthUpdateConsumer(DepthService depthService) {
        this.depthService = depthService;
    }

    @KafkaListener(
            topics = "${depth.kafka.topic:depth-updates}",
            containerFactory = "depthKafkaListenerContainerFactory"
    )
    public void onDepthUpdate(byte[] payload) {
        depthService.processUpdate(payload);
    }
}
