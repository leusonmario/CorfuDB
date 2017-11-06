package org.corfudb.infrastructure.orchestrator.workflows;

import lombok.Getter;
import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.Workflow;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * A workflow that adds a new node to the cluster.
 *
 * Created by Maithem on 10/25/17.
 */

public class AddNode extends Workflow {

    @Getter
    String endpoint;

    public AddNode(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public List<Action> getActions() {
        List<Action> actions = new ArrayList<>();
        actions.add(new SplitSegment());
        actions.add(new TransferSegment());
        actions.add(new MergeSegment());
        return actions;
    }

    class SplitSegment extends Action {

        @Override
        public String getName() {
            return getClass().getName();
        }

        @Override
        public void execute(CorfuRuntime runtime) {

        }
    }

    class TransferSegment extends Action {

        @Override
        public String getName() {
            return getClass().getName();
        }

        @Override
        public void execute(CorfuRuntime runtime) {

        }
    }

    class MergeSegment extends Action {

        @Override
        public String getName() {
            return getClass().getName();
        }

        @Override
        public void execute(CorfuRuntime runtime) {

        }
    }
}
