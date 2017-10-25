package org.corfudb.infrastructure.orchestrator;

import org.corfudb.runtime.CorfuRuntime;

public abstract class Action {
    public abstract String getName();
    public abstract void execute(CorfuRuntime runtime);
}
