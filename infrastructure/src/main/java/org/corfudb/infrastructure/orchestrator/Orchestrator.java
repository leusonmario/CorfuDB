package org.corfudb.infrastructure.orchestrator;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.orchestrator.workflows.AddNode;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorRequest;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The orchestrator is a stateless service that runs on all management servers and its purpose
 * is to execute workflows. A workflow defines multiple smaller actions that run on the cluster
 * in order to achieve a bigger goal. For example, growing the cluster. Initiated through RPC,
 * the orchestrator will create a workflow instance and attempt to execute all its actions.
 *
 * <p>
 * Created by Maithem on 10/25/17.
 */

@Slf4j
public class Orchestrator {

    final CorfuRuntime runtime;

    public Orchestrator(@Nonnull CorfuRuntime runtime) {
        this.runtime = runtime;
    }

    public void handle(@Nonnull CorfuPayloadMsg<OrchestratorRequest> msg,
                       @Nonnull ChannelHandlerContext ctx,
                       @Nonnull IServerRouter r) {

        OrchestratorRequest req = msg.getPayload();
        dispatch(req);
        r.sendResponse(ctx, msg, CorfuMsgType.ORCHESTRATOR_RESPONSE.msg());
    }

    void dispatch(@Nonnull OrchestratorRequest req) {
        CompletableFuture.runAsync(() -> {
            run(getWorkflow(req));
        });
    }

    /**
     * Create a workflow instance from an orchestrator request
     * @param req Orchestrator request
     * @return Workflow instance
     */
    @Nonnull
    private Workflow getWorkflow(@Nonnull OrchestratorRequest req) {
        switch (req.getWorkFlowType()) {
            case ADD_NODE:
                AddNodeRequest addNodeRequest = (AddNodeRequest) req;
                return new AddNode(addNodeRequest.getEndpoint());
            default:
                throw new RuntimeException("Unknown workflow");
        }
    }

    /**
     * Run a particular workflow, which entails executing all its defined
     * actions
     * @param workflow instance to run
     */
    void run(@Nonnull Workflow workflow) {

        log.info("Started workflow {}", workflow.getName());

        for (Action action : workflow.getActions()) {
            try {
                log.info("Action {} Status {}", action.getName(), ActionStatus.STARTED);
                action.execute(runtime);
                log.info("Action {} Status {}", action.getName(), ActionStatus.COMPLETED);
            } catch (Exception e) {
                log.info("Action {} Status {} {}", action.getName(), ActionStatus.ERROR, e);
                return;
            }
        }
        log.info("Completed workflow {}", workflow.getName());
    }
}
