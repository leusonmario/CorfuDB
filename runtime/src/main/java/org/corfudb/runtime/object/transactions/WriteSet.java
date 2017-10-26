package org.corfudb.runtime.object.transactions;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.IObjectManager;

/**
 * This class captures information about objects mutated (written) during speculative
 * transaction execution.
 */
@Getter
public class WriteSet extends ConflictSet {

    /** The actual updates to mutated objects. */
    final MultiObjectSMREntry writeSet = new MultiObjectSMREntry();


    public <T> long add(IObjectManager<T> manager, SMREntry updateEntry, Object[] conflictObjects) {
        super.add(manager, conflictObjects);
        writeSet.addTo(manager.getId(), updateEntry);
        return writeSet.getSMRUpdates(manager.getId()).size() - 1;
    }

}
