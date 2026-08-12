package com.pfm.processing.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.pfm.common.domain.NetPosition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/** JSON serde for the aggregate value, configured identically to {@link TransactionSerde}. */
public final class NetPositionSerde {

    private NetPositionSerde() {
    }

    public static Serde<NetPosition> instance() {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        JsonSerializer<NetPosition> serializer = new JsonSerializer<>(objectMapper);
        JsonDeserializer<NetPosition> deserializer =
                new JsonDeserializer<>(NetPosition.class, objectMapper, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
