package org.corfudb.runtime.object.transactions;

import java.util.UUID;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineAccess;
import org.corfudb.runtime.object.IStateMachineOp;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.object.VersionedObjectManager;
import org.corfudb.runtime.view.ObjectBuilder;

/** A write-after-write transactional context.
 *
 * <p>A write-after-write transactional context behaves like an optimistic
 * context, except behavior during commit (for writes):
 *
 *   <p>(1) Reads behave the same as in a regular optimistic
 *     transaction.
 *
 *   <p>(2) Writes in a write-after-write transaction are guaranteed
 *     to commit atomically, if and only if none of the objects
 *     written (the "write set") were modified between the first read
 *     ("first read timestamp") and the time of commit.
 *
 * <p>Created by mwei on 11/21/16.
 */
@Slf4j
public class WriteAfterWriteTransaction
        extends AbstractOptimisticTransaction {

    WriteAfterWriteTransaction(TransactionBuilder builder, AbstractTransaction parent) {
        super(builder, parent);
        obtainSnapshotTimestamp();
    }


    @Override
    public IStateMachineStream getStateMachineStream(IObjectManager manager,
                                                     IStateMachineStream current) {
        if (current instanceof WriteAfterWriteStateMachineStream
                && ((WriteAfterWriteStateMachineStream) current).writerContext.isActive()) {
            return current;
        }
        return new WriteAfterWriteStateMachineStream(manager, current.getRoot(), Transactions
                .getReadSnapshot());
    }



}
