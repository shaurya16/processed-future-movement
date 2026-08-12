package com.pfm.ingestion.kafka;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = {"spring.kafka.admin.auto-create=false", "ingestion.topic=future-transactions"})
class KafkaConfigTest {

    @Autowired
    KafkaTemplate<String, FutureTransaction> kafkaTemplate;

    @Autowired
    NewTopic futureTransactionsTopic;

    @Test
    void kafkaTemplateBeanExists() {
        assertNotNull(kafkaTemplate);
    }

    @Test
    void topicIsConfiguredWithThreePartitionsAndReplicationFactorOne() {
        assertEquals("future-transactions", futureTransactionsTopic.name());
        assertEquals(3, futureTransactionsTopic.numPartitions());
        assertEquals(1, futureTransactionsTopic.replicationFactor());
    }
}
