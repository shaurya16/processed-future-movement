package com.pfm.processing.streams;

import com.pfm.common.domain.NetPosition;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetPositionSerdeTest {

    private static final String TOPIC = "future-transactions";

    @Test
    void roundTripsEveryField() {
        NetPosition original = new NetPosition(
                -215L, 285L, 500L, 12,
                LocalDate.of(2010, 8, 19), LocalDate.of(2010, 8, 20),
                Instant.parse("2026-08-12T14:31:52Z"),
                Map.of("USD", new BigDecimal("-0.90")));

        Serde<NetPosition> serde = NetPositionSerde.create();
        NetPosition restored = serde.deserializer()
                .deserialize(TOPIC, serde.serializer().serialize(TOPIC, original));

        assertEquals(original, restored);
    }

    @Test
    void roundTripsAnEmptyPositionWithNullDates() {
        Serde<NetPosition> serde = NetPositionSerde.create();
        NetPosition restored = serde.deserializer()
                .deserialize(TOPIC, serde.serializer().serialize(TOPIC, NetPosition.empty()));

        assertEquals(NetPosition.empty(), restored);
    }

    @Test
    void serializesTemporalFieldsAsIsoStringsNotNumericTimestamps() {
        NetPosition position = new NetPosition(1L, 1L, 0L, 1,
                LocalDate.of(2010, 8, 19), LocalDate.of(2010, 8, 19),
                Instant.parse("2026-08-12T14:31:52Z"), Map.of());

        String json = new String(NetPositionSerde.create().serializer().serialize(TOPIC, position));

        assertTrue(json.contains("\"2010-08-19\""), json);
        assertTrue(json.contains("2026-08-12T14:31:52Z"), json);
    }
}
