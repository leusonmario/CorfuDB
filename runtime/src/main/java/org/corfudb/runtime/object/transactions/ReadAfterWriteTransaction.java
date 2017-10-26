package org.corfudb.runtime.object.transactions;

import java.util.UUID;

import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineAccess;
import org.corfudb.runtime.object.IStateMachineStream;

public class ReadAfterWriteTransaction
        extends AbstractOptimisticTransaction {

    @Override
    public IStateMachineStream getStateMachineStream(IObjectManager manager,
                                                     IStateMachineStream current) {
        if (current instanceof ReadAfterWriteStateMachineStream
                && ((ReadAfterWriteStateMachineStream) current).writerContext.isActive()) {
            return current;
        }
        return new ReadAfterWriteStateMachineStream(manager, current.getRoot(),
                obtainSnapshotTimestamp());
    }


    public ReadAfterWriteTransaction(TransactionBuilder builder, AbstractTransaction parent) {
        super(builder, parent);
    }

}
