package org.corfudb.infrastructure.orchestrator;

import java.util.List;

public abstract class Workflow {

    public abstract String getName();
    public abstract List<Action> getActions();
}
