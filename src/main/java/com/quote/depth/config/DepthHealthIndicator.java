package com.quote.depth.config;

import com.quote.depth.service.DepthService;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports DOWN until at least one depth SNAPSHOT has been received
 * from Kafka, preventing load balancers from routing traffic to a
 * node whose cache is still empty.
 */
@Component
public class DepthHealthIndicator implements HealthIndicator {

    private final DepthService depthService;

    public DepthHealthIndicator(DepthService depthService) {
        this.depthService = depthService;
    }

    @Override
    public Health health() {
        if (depthService.isWarm()) {
            return Health.up()
                    .withDetail("symbols", depthService.cachedSymbolCount())
                    .build();
        }
        return Health.down()
                .withDetail("reason", "waiting for first SNAPSHOT from Kafka")
                .build();
    }
}
