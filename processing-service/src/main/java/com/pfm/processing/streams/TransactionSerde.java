package com.pfm.processing.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public final class TransactionSerde {

    private TransactionSerde() {
    }

    public static Serde<FutureTransaction> instance() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JsonSerializer<FutureTransaction> serializer = new JsonSerializer<>(objectMapper);
        JsonDeserializer<FutureTransaction> deserializer =
                new JsonDeserializer<>(FutureTransaction.class, objectMapper, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
