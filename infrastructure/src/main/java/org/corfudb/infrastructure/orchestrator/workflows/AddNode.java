package org.corfudb.infrastructure.orchestrator.workflows;

import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.Workflow;

import java.util.ArrayList;
import java.util.List;

public class AddNode extends Workflow {

    @Override
    public String getName() {
        return this.getClass().getName();
    }

    @Override
    public List<Action> getActions() {
        return new ArrayList<>();
    }
}
