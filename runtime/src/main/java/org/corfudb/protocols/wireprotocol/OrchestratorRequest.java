package org.corfudb.protocols.wireprotocol;


import lombok.Getter;
import io.netty.buffer.ByteBuf;
import org.corfudb.format.Types.WorkFlowType;

public class OrchestratorRequest implements ICorfuPayload<OrchestratorRequest> {

    @Getter
    WorkFlowType type;

    public OrchestratorRequest(ByteBuf buf) {

    }

    public OrchestratorRequest(WorkFlowType type) {
        this.type = type;
    }


    @Override
    public void doSerialize(ByteBuf buf) {

    }
}
