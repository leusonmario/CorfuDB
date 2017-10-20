package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Raw Log Entries map payload.
 */
@Data
@AllArgsConstructor
public class RawDataMsg implements ICorfuPayload<RawDataMsg> {

    private Map<Long, byte[]> rawDataMap;

    public RawDataMsg(ByteBuf buf) {
        rawDataMap = ICorfuPayload.mapFromBuffer(buf, Long.class, byte[].class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, rawDataMap);
    }
}
