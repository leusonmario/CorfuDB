package org.corfudb.infrastructure.orchestrator.registry;

import org.corfudb.infrastructure.orchestrator.Action;
import org.corfudb.infrastructure.orchestrator.Status;
import org.corfudb.infrastructure.orchestrator.Workflow;

import java.util.UUID;

public interface Registry {

    WorkflowEntry create(String name, UUID owner);

    void update(UUID id, Action action, Status status);

    WorkflowEntry get(String uuid);
}
