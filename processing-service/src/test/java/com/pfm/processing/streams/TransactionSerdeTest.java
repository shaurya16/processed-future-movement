package com.pfm.processing.streams;

import com.pfm.common.domain.FutureTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransactionSerdeTest {

    @Test
    void roundTripsAFutureTransactionThroughSerializeAndDeserialize() {
        Serde<FutureTransaction> producerSide = TransactionSerde.instance();
        Serde<FutureTransaction> consumerSide = TransactionSerde.instance();
        FutureTransaction original = new FutureTransaction(
                "315", "CL", "4321", "0002", "0001", "SGXDC", "FU", "SGX", "NK",
                LocalDate.of(2010, 9, 10), "JPY", "01", 'B',
                100L, 30L,
                BigDecimal.valueOf(60, 2), "USD", 'D',
                BigDecimal.valueOf(30, 2), "USD", 'D',
                BigDecimal.ZERO, "JPY", 'D',
                LocalDate.of(2010, 8, 20), "001238", "0", "688032",
                BigDecimal.valueOf(925, 5), "TRDR12", "OPP0001", 'O');

        byte[] bytes = producerSide.serializer().serialize("future-transactions", original);
        FutureTransaction roundTripped = consumerSide.deserializer().deserialize("future-transactions", bytes);

        assertEquals(original, roundTripped);
    }
}
