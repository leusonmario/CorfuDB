package org.corfudb.runtime.object.transactions;

import javax.annotation.Nonnull;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineStream;

public class WriteAfterWriteStateMachineStream extends AbstractOptimisticStateMachineStream {

    public WriteAfterWriteStateMachineStream(@Nonnull IObjectManager<?> manager,
                                             @Nonnull IStateMachineStream parent, long address) {
        super(manager, parent, address);
    }

    @Override
    public long append(String smrMethod, Object[] smrArguments,
                       Object[] conflictObjects, boolean keepEntry) {
        Transactions.getContext().getConflictSet().add(manager, conflictObjects);
        return super.append(smrMethod, smrArguments, conflictObjects, keepEntry);
    }


}
