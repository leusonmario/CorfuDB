package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineAccess;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.object.VersionedObjectManager;

/**
 * A snapshot transactional context.
 *
 * <p>Given the snapshot (log address) given by the TransactionBuilder,
 * access all objects within the same snapshot during the course of
 * this transactional context.
 *
 * <p>Created by mwei on 11/22/16.
 */
public class SnapshotTransaction extends AbstractTransaction {

    final long previousSnapshot;

    public SnapshotTransaction(TransactionBuilder builder, AbstractTransaction parent) {
        super(builder, parent);
        previousSnapshot = Transactions.getReadSnapshot();
        Transactions.getContext().setReadSnapshot(builder.snapshot);
    }

    @Override
    public IStateMachineStream getStateMachineStream(IObjectManager manager,
                                                     IStateMachineStream current) {
        if (current instanceof SnapshotStateMachineStream
                && ((SnapshotStateMachineStream) current).snapshotAddress ==
                Transactions.getReadSnapshot()) {
            return current;
        }
        return new SnapshotStateMachineStream(current.getRoot(), Transactions.getReadSnapshot());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long commit() throws TransactionAbortedException {
        // Restore the previous snapshot on commit.
        Transactions.getContext().setReadSnapshot(previousSnapshot);
        return super.commit();
    }

}
