package org.corfudb.infrastructure.orchestrator.workflows;

import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.Workflow;
import org.corfudb.infrastructure.orchestrator.registry.WorkflowEntry;
import org.corfudb.runtime.CorfuRuntime;

import java.util.ArrayList;
import java.util.List;

public class AddNode extends Workflow {

    WorkflowEntry entry;

    public AddNode(WorkflowEntry entry) {
        this.entry = entry;
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
