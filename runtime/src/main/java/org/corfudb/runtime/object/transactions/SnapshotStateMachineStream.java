package org.corfudb.runtime.object.transactions;

import java.util.UUID;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.IStateMachineOp;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.view.Address;

import lombok.Getter;

public class SnapshotStateMachineStream implements IStateMachineStream {

    @Getter
    final IStateMachineStream parent;

    long snapshotAddress;

    public SnapshotStateMachineStream(@Nonnull IStateMachineStream parent, long address) {
        this.parent = parent;
        snapshotAddress = address;
    }

    @Nonnull
    @Override
    public Stream<IStateMachineOp> sync(long pos, @Nullable Object[] conflictObjects) {
        if (pos == Address.OPTIMISTIC) {
            return Stream.empty();
        } else if (pos != Address.MAX) {
          throw new UnsupportedOperationException("Snapshot stream cannot sync to position " + pos);
        } else if (parent.pos() == snapshotAddress) {
            return Stream.empty();
        } else {
            Stream<IStateMachineOp> stream = parent.sync(snapshotAddress, conflictObjects);
            // Remember the absolute pos (which can be < the original snapshot address).
            snapshotAddress = parent.pos();
            return stream;
        }
    }

    @Override
    public long pos() {
        return parent.pos();
    }

    @Override
    public void reset() {
        parent.reset();
    }

    @Override
    public long check() {
        return parent.pos() == snapshotAddress ? Address.UP_TO_DATE : Address.MAX;
    }

    @Override
    public long append(String smrMethod, Object[] smrArguments,
                       Object[] conflictObjects, boolean keepEntry) {
        throw new UnsupportedOperationException("Snapshot stream doesn't support append");
    }

    @Override
    public IStateMachineOp consumeEntry(long address) {
        throw new UnsupportedOperationException("Snapshot stream cannot keep entries");
    }

    @Override
    public UUID getId() {
        return parent.getId();
    }
}
