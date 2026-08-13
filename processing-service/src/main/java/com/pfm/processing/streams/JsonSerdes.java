package com.pfm.processing.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * Builds the JSON serdes the topology reads and writes, so the Jackson configuration
 * is stated once. It has to match what ingestion-service's producer emits — dates as
 * ISO strings, not epoch numbers — and previously the same three lines were repeated
 * per serde, which is where a mapper change gets applied to one side and not the other.
 *
 * <p>Returns a fresh serde per call rather than a shared instance: Kafka Streams
 * configures and closes the serdes it is handed, so one shared instance across the
 * source, the groupBy and the store would couple their lifecycles. The mapper itself
 * is immutable once configured and safe to share.
 */
final class JsonSerdes {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private JsonSerdes() {
    }

    static <T> Serde<T> create(Class<T> type) {
        JsonSerializer<T> serializer = new JsonSerializer<>(OBJECT_MAPPER);
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(type, OBJECT_MAPPER, false);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
