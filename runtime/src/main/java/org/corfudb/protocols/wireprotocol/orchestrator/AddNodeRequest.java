package org.corfudb.protocols.wireprotocol.orchestrator;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.corfudb.format.Types.WorkflowType;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

/**
 * Add node request.
 *
 * Created by Maithem on 10/25/17.
 */

public class AddNodeRequest extends OrchestratorRequest implements ICorfuPayload<AddNodeRequest> {

    @Getter
    String endpoint;

    @Getter
    WorkflowType workflowType;

    public AddNodeRequest(ByteBuf buf) {
        workflowType = WorkflowType.forNumber(buf.readInt());
        int endpointLen = buf.readInt();
        byte[] endpointBytes = new byte[endpointLen];
        buf.readBytes(endpointBytes);
        endpoint = new String(endpointBytes);
    }

    public AddNodeRequest(String endpoint) {
        this.endpoint = endpoint;
        this.workflowType = WorkflowType.ADD_NODE;
    }

    @Override
    public WorkflowType getWorkFlowType() {
        return workflowType;
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeInt(WorkflowType.ADD_NODE.getNumber());
        buf.writeInt(endpoint.length());
        buf.writeBytes(endpoint.getBytes());
    }
}
