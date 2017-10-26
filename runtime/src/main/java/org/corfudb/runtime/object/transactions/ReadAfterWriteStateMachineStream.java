package org.corfudb.runtime.object.transactions;

import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineOp;
import org.corfudb.runtime.object.IStateMachineStream;

public class ReadAfterWriteStateMachineStream extends AbstractOptimisticStateMachineStream {

    public ReadAfterWriteStateMachineStream(@Nonnull IObjectManager manager,
                                            @Nonnull IStateMachineStream parent, long address) {
        super(manager, parent, address);
    }

    @Nonnull
    @Override
    public Stream<IStateMachineOp> sync(long max, @Nullable Object[] conflictObjects) {
        Transactions.getContext().getConflictSet().add(manager,  conflictObjects);
        return super.sync(max, conflictObjects);
    }

}
