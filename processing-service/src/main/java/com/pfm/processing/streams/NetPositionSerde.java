package com.pfm.processing.streams;

import com.pfm.common.domain.NetPosition;
import org.apache.kafka.common.serialization.Serde;

/** JSON serde for the aggregate value held in the state store. */
public final class NetPositionSerde {

    private NetPositionSerde() {
    }

    /** A new serde per call — named create(), not instance(), because it allocates. */
    public static Serde<NetPosition> create() {
        return JsonSerdes.create(NetPosition.class);
    }
}
