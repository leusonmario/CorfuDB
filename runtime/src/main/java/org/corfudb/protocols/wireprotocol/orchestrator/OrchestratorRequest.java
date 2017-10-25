package org.corfudb.protocols.wireprotocol.orchestrator;

import org.corfudb.format.Types.WorkflowType;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;

/**
 * A message container that all orchestrator requests should extend
 *
 * Created by Maithem on 10/25/17.
 */

public abstract class OrchestratorRequest {
    public abstract WorkflowType getWorkFlowType();
}
