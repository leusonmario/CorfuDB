package org.corfudb.infrastructure.orchestrator;

import java.util.List;

/**
 *
 * A workflow is an abstract container that specifies a series of steps that achieves a
 * multi-step operation. For example, adding a new node to the cluster, which requires
 * several steps to complete.
 *
 * Created by Maithem on 10/25/17.
 */

public abstract class Workflow {

    /**
     * Return the name of this workflow
     * @return workflow's name
     */
    public abstract String getName();

    /**
     * Returns the actions that are associated with
     * this workflow.
     * @return List of actions
     */
    public abstract List<Action> getActions();
}
