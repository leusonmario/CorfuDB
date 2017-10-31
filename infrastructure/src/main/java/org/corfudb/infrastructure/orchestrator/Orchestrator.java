package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.orchestrator.registry.Registry;
import org.corfudb.infrastructure.orchestrator.registry.WorkflowEntry;
import org.corfudb.infrastructure.orchestrator.workflows.AddNode;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.OrchestratorRequest;
import org.corfudb.runtime.CorfuRuntime;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class Orchestrator {

    final CorfuRuntime runtime;

    final Registry registry;

    final UUID id;

    public Orchestrator(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.registry = null;
        this.id = UUID.randomUUID();
    }

    public void handle(CorfuPayloadMsg<OrchestratorRequest> msg,
                       ChannelHandlerContext ctx, IServerRouter r) {

        try {
            OrchestratorRequest req = msg.getPayload();
            WorkflowEntry entry = registry.create(req.getType().toString(), id);
            dispatch(entry);

        } catch (Exception e) {

        }
    }

    void dispatch(WorkflowEntry entry) {
        CompletableFuture.runAsync( () -> run(entry));
    }

    void run(WorkflowEntry entry) {



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
