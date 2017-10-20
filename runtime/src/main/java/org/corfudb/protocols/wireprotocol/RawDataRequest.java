package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Request to transfer LogEntries in the address range.
 */
@Data
@AllArgsConstructor
public class RawDataRequest implements ICorfuPayload<RawDataRequest> {

    private long startAddress;
    private long endAddress;

    public RawDataRequest(ByteBuf buf) {
        startAddress = ICorfuPayload.fromBuffer(buf, Long.class);
        endAddress = ICorfuPayload.fromBuffer(buf, Long.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, startAddress);
        ICorfuPayload.serialize(buf, endAddress);
    }
}
