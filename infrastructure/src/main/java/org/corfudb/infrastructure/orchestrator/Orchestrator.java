package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.orchestrator.registry.Registry;
import org.corfudb.infrastructure.orchestrator.registry.WorkflowEntry;
import org.corfudb.infrastructure.orchestrator.workflows.AddNode;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.OrchestratorRequest;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class Orchestrator {

    final CorfuRuntime runtime;

    final Registry registry;

    final UUID id;

    //final Map<String, Workflow> workflows;

    public Orchestrator(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.registry = null;
        this.id = UUID.randomUUID();
    }

    public void handle(CorfuPayloadMsg<OrchestratorRequest> msg,
                       ChannelHandlerContext ctx, IServerRouter r) {

        OrchestratorRequest req = msg.getPayload();

        switch (req.getType()) {
            case ADD_NODE:
                try {
                    WorkflowEntry entry = registry.create(req.getType().toString(), id);
                    dispatch(entry);
                    r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
                } catch (Exception e) {
                    log.error("Add Node Error: {}", e);
                    r.sendResponse(ctx, msg, CorfuMsgType.ERROR_SERVER_EXCEPTION.msg());
                }

                break;
            default:
                break;
        }
    }

    void dispatch(WorkflowEntry entry) {
        CompletableFuture.runAsync( () -> run(entry));
    }

    void run(WorkflowEntry entry) {
        log.info("Started workflow {} owner {} id {}", entry.getWorkflowName(), entry.getOwner(), entry.getId());

        // Select workflow
        AddNode workflow = new AddNode(entry);

        // Complete Workflow or Start a new one

        for (Action action : workflow.getActions()) {
            try {
                log.info("executing action {} for {}", action.getName(), id);
                registry.update(entry.getOwner(), action, Status.STARTED);
                action.execute(runtime);
                registry.update(entry.getOwner(), action, Status.COMPLETED);
            } catch (Exception e) {
                // change action status to error
                // should be a non-registry error
                log.error("{}", e);
                registry.update(entry.getOwner(), action, Status.ERROR);
                break;
            }
        }
        log.info("Completed workflow {} id {}", workflow.getName(), id);
    }
}
