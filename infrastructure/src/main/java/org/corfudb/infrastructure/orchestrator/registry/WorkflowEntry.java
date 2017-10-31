package org.corfudb.infrastructure.orchestrator.registry;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class WorkflowEntry {

    @Getter
    UUID owner;

    @Getter
    String workflowName;

    @Getter
    List<ActionEntry> actions = new ArrayList<>();

    public WorkflowEntry(UUID owner, String workflowName) {
        this.owner = owner;
        this.workflowName = workflowName;
    }
}
