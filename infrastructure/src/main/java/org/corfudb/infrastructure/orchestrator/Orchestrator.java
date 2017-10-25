package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.orchestrator.workflows.AddNode;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.OrchestratorRequest;
import org.corfudb.runtime.CorfuRuntime;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class Orchestrator {

    final CorfuRuntime runtime;

    public Orchestrator(CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    public void handle(CorfuPayloadMsg<OrchestratorRequest> msg,
                       ChannelHandlerContext ctx, IServerRouter r) {
        // return id ?
        OrchestratorRequest req = msg.getPayload();

        switch (req.getType()) {
            case ADD_NODE:
                CompletableFuture.runAsync( () -> run(new AddNode()));
                break;
            case REMOVE_NODE:
                break;
            default:
                break;
        }
    }

    void run(Workflow workflow) {
        // Lock to this node?
        UUID id = UUID.randomUUID();
        log.info("Started workflow {} id {}", workflow.getName(), id);

        // create workflow instance in datastore and generate id
        for (Action action : workflow.getActions()) {
            try {
                log.info("executing action {} for {}", action.getName(), id);
                action.execute(runtime);
            } catch (Exception e) {
                // change action status to error
                break;
            }
        }
        log.info("Completed workflow {} id {}", workflow.getName(), id);
    }
}
