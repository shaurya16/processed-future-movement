package com.pfm.processing.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaStreamsHealthIndicatorTest {

    @Test
    void reportsUpWhenKafkaStreamsIsRunning() {
        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.RUNNING);

        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        Health health = new KafkaStreamsHealthIndicator(factoryBean).health();

        assertEquals(Status.UP, health.getStatus());
    }

    @Test
    void reportsDownWhenKafkaStreamsIsInErrorState() {
        KafkaStreams kafkaStreams = mock(KafkaStreams.class);
        when(kafkaStreams.state()).thenReturn(KafkaStreams.State.ERROR);

        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(kafkaStreams);

        Health health = new KafkaStreamsHealthIndicator(factoryBean).health();

        assertEquals(Status.DOWN, health.getStatus());
    }

    @Test
    void reportsDownWhenKafkaStreamsHasNotStartedYet() {
        StreamsBuilderFactoryBean factoryBean = mock(StreamsBuilderFactoryBean.class);
        when(factoryBean.getKafkaStreams()).thenReturn(null);

        Health health = new KafkaStreamsHealthIndicator(factoryBean).health();

        assertEquals(Status.DOWN, health.getStatus());
    }
}
