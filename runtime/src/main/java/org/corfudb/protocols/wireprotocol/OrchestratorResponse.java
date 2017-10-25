package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

public class OrchestratorResponse implements ICorfuPayload<OrchestratorResponse> {

    public OrchestratorResponse() {

    }

    public OrchestratorResponse(ByteBuf buf) {

    }

    @Override
    public void doSerialize(ByteBuf buf) {

    }
}
