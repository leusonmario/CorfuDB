package org.corfudb.infrastructure;

import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.workflow.Step;

@Slf4j
public class ManagementWorkflowSteps {

    public static CorfuRuntime corfuRuntime;
    public static FailureHandlerDispatcher failureHandlerDispatcher;

    public enum ManagementSteps {

        SET_UP_NODE(setUpNewNode()),
        ADD_NODE(addNode());

        @Getter
        private final Step step;

        ManagementSteps(Step step) {
            this.step = step;
        }
    }

    public static Step<Map<String, String>, Boolean> setUpNewNode() {
        return new Step<>("SET_UP_NODE", map -> {

            String endpoint = map.get("endpoint");

            // Bootstrap the to-be added node with the old layout.
            IClientRouter newEndpointRouter = corfuRuntime.getRouter(endpoint);
            try {
                Layout layout = (Layout) corfuRuntime.getLayoutView().getLayout().clone();
                // Ignoring call result as the call returns ACK or throws an exception.
                newEndpointRouter.getClient(LayoutClient.class)
                        .bootstrapLayout(layout).get();
                newEndpointRouter.getClient(ManagementClient.class)
                        .bootstrapManagement(layout).get();
                newEndpointRouter.getClient(ManagementClient.class)
                        .initiateFailureHandler().get();

                log.info("handleAddNodeRequest: New node {} bootstrapped.", endpoint);
            } catch (Exception e) {
                log.error("handleAddNodeRequest: "
                        + "Aborting as new node could not be bootstrapped : ", e);
                throw new RuntimeException(e);
            }
            return true;
        });
    }

    public static Step<Map<String, String>, Boolean> addNode() {
        return new Step<>("ADD_NODE", map -> {

            Layout layout = corfuRuntime.getLayoutView().getLayout();
            String endpoint = map.get("endpoint");
            boolean isLayoutServer = Boolean.parseBoolean(map.get("isLayoutServer"));
            boolean isSequencerServer = Boolean.parseBoolean(map.get("isSequencerServer"));
            boolean isLogUnitServer = Boolean.parseBoolean(map.get("isLogUnitServer"));
            boolean isUnresponsiveServer = Boolean.parseBoolean(map.get("isUnresponsiveServer"));
            int logUnitStripeIndex = Integer.parseInt(map.get("logUnitStripeIndex"));

            try {
                // Fill layout slot if failure in maxGlobalLogTail
                // Not wait for another failure detector.
                failureHandlerDispatcher.addNode(layout, corfuRuntime, endpoint, isLayoutServer,
                        isSequencerServer, isLogUnitServer, isUnresponsiveServer,
                        logUnitStripeIndex);
            } catch (Exception e) {
                log.error("Request to add new node: {} failed with exception:", map, e);
                throw new RuntimeException(e);
            }
            return true;
        });
    }
}
