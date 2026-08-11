package com.pfm.processing.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

@Component
public class KafkaStreamsHealthIndicator implements HealthIndicator {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public KafkaStreamsHealthIndicator(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @Override
    public Health health() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            return Health.down().withDetail("state", "NOT_STARTED").build();
        }

        KafkaStreams.State state = kafkaStreams.state();
        if (state.isRunningOrRebalancing()) {
            return Health.up().withDetail("state", state.name()).build();
        }
        return Health.down().withDetail("state", state.name()).build();
    }
}
