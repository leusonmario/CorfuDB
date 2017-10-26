package org.corfudb.runtime.object.transactions;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineOp;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectBuilder;

public abstract class AbstractOptimisticStateMachineStream extends SnapshotStateMachineStream {


    long optimisticPos = Address.NEVER_READ;
    protected final IObjectManager<?> manager;
    final TransactionContext writerContext;

    public AbstractOptimisticStateMachineStream(@Nonnull IObjectManager<?> manager,
                                                @Nonnull IStateMachineStream parent,
                                                long address) {
        super(parent, address);
        this.manager = manager;
        this.writerContext = Transactions.getContext();
    }

    @Nonnull
    @Override
    public Stream<IStateMachineOp> sync(long pos, @Nullable Object[] conflictObjects) {
        final List<SMREntry> updateList = writerContext.getWriteSet().getWriteSet()
                .getSMRUpdates(parent.getId());
        final long previousPos = optimisticPos;

        // Revert all optimistic updates
        if (pos == Address.OPTIMISTIC) {
            // If there are no optimistic updates (or updates were never applied),
            // we don't need to do anything!
            if (optimisticPos == Address.NEVER_READ || optimisticPos == 0) {
                optimisticPos = Address.NEVER_READ;
                return Stream.empty();
            }

            // Reset the optimistic position
            optimisticPos = Address.NEVER_READ;

            // If the optimistic updates have been committed, "rollback" means just
            // syncing forward to the commit position, ignoring the write set at this
            // address.
            if (writerContext.getCommitAddress() != Address.OPTIMISTIC) {
                return super.getRoot().sync(writerContext.getCommitAddress(), null)
                        .filter(x -> x.getAddress() != writerContext.getCommitAddress());
            }

            // Otherwise, build a reversed stream over list of optimistic undo records.
            return IntStream.rangeClosed(1, (int)previousPos)
                    .mapToObj(i -> updateList.get((int)previousPos - i).getUndoOperation());
        } else if (pos != Address.MAX) {
            throw new UnsupportedOperationException("Optimistic stream cannot sync to position");
        }

        optimisticPos = updateList.size();
        if (previousPos == Address.NEVER_READ) {
            return Stream.concat(super.sync(pos, conflictObjects), updateList.stream());
        }

        return updateList.subList((int) previousPos, updateList.size()).stream()
                .map(x -> (IStateMachineOp) x);
    }

    @Override
    public long check() {
        long base = super.check();
        if (base == Address.UP_TO_DATE
                && (optimisticPos == Address.NEVER_READ
                    || optimisticPos == writerContext.getWriteSet().getWriteSet()
                .getSMRUpdates(parent.getId()).size())) {
            return Address.UP_TO_DATE;
        }
        return Address.MAX;
    }

    @Override
    public long append(String smrMethod, Object[] smrArguments,
                       Object[] conflictObjects, boolean keepEntry) {
        SMREntry smrEntry = new SMREntry(smrMethod, smrArguments,
                ((ObjectBuilder)manager.getBuilder()).getSerializer());
        return Transactions.getContext().getWriteSet().add(manager, smrEntry, conflictObjects);
    }


    @Override
    public IStateMachineOp consumeEntry(long address) {
        return Transactions.getContext().getWriteSet().getWriteSet()
                .getSMRUpdates(parent.getId()).get((int) address);
    }

}
