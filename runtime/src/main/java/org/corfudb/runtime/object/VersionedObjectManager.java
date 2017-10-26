package org.corfudb.runtime.object;

import static java.lang.Math.min;

import java.util.UUID;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.TrimmedUpcallException;
import org.corfudb.runtime.object.transactions.AbstractTransaction;
import org.corfudb.runtime.object.transactions.Transactions;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.Utils;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The VersionedObjectManager maintains a versioned state-machine object {@code object}.
 * This object can time-travel and be optimistically updated.
 *
 * <p>To access object state, users call {@link #access(IStateMachineAccess, Object[])} with
 * a function that is executed when the object is "sync'd". The definition of sync is context
 * dependent. For example, if a transaction is active, it will depend on the type of transaction.
 * Typically, the VersionedObjectManager is instantiated with a
 * {@link LinearizableStateMachineStream}, which means that an access will reflect any updates
 * added via the {@link #logUpdate(String, boolean, Object[], Object...)} operation ordered before
 * it (in wall-clock time).
 *
 * <p>To modify an object, users call {@link #logUpdate(String, boolean, Object[], Object...)} with
 * a proposed state machine update. This function returns an address, which can be used in a
 * later call to {@link #getUpcallResult(long, Object[])}. This is used if the update returns a
 * value.
 *
 */
@Slf4j
public class VersionedObjectManager<T> implements IObjectManager<T> {

    /**
     * The actual underlying object.
     */
    T object;

    /**
     * A lock, which controls access to modifications to
     * the object. Any access to unsafe methods should
     * obtain the lock.
     */
    private final StampedLock lock;

    /**
     * The stream view this object is backed by.
     */
    private IStateMachineStream smrStream;

    /**
     * The wrapper for this object.
     */
    @Getter
    private final ICorfuWrapper<T> wrapper;

    /** The builder for the object.
     *
     */
    @Getter
    private final ObjectBuilder<T> builder;

    /**
     * Construct a new versioned object manager with the given stream, wrapper and builder.
     */
    public VersionedObjectManager(IStateMachineStream smrStream,
                                  ICorfuWrapper<T> wrapper,
                                  ObjectBuilder<T> builder) {
        this.builder = builder;
        this.smrStream = smrStream;
        this.wrapper = wrapper;
        this.object = builder.getRawInstance();

        lock = new StampedLock();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long logUpdate(String smrUpdateFunction,
                          boolean keepUpcallResult,
                          Object[] conflictObject,
                          Object... args) {
        final IStateMachineStream stream = getActiveStream();
        return stream.append(smrUpdateFunction, args, conflictObject, keepUpcallResult);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <R> R getUpcallResult(long address, Object[] conflictObject) {
        long ts = 0;
            try {
                IStateMachineStream stream = getActiveStream();
                IStateMachineOp entry = stream.consumeEntry(address);

                // If there is no upcall present, we must obtain it (via syncing).
                if (!entry.isUpcallResultPresent()) {
                    // If the object is write-locked, this means some other thread is syncing
                    // the object forward. This might have our update, so we take a read-lock
                    // instead of a write lock in this case.
                    if (lock.isWriteLocked()) {
                        // Perhaps the last writer calculated the upcall for us
                        ts = lock.readLock();
                        try {
                            entry = stream.consumeEntry(address);
                            if (entry.isUpcallResultPresent()) {
                                return (R) entry.getUpcallResult();
                            }
                        } finally {
                            lock.unlock(ts);
                        }
                    }
                    try {
                        ts = lock.writeLock();
                        checkAndSwitchStreamsUnsafe();
                        // Potentially pick up other updates.
                        syncObjectUnsafe(Address.MAX, conflictObject);
                        entry = stream.consumeEntry(address);
                    } finally {
                        lock.unlock(ts);
                    }
                }

                if (entry.isUpcallResultPresent()) {
                    return (R) entry.getUpcallResult();
                }
                throw new RuntimeException("Attempted to get the result "
                        + "of an upcall@" + address + " but we are @"
                        + getVersionUnsafe()
                        + " and we don't have a copy");
            } catch (TrimmedException te) {
                throw new TrimmedUpcallException(address);
            }
    }


    /** {@inheritDoc} */
    @Override
    public <R> R access(IStateMachineAccess<R, T> accessFunction,
                           Object[] conflictObject) {
        // Try an optimistic read first.
        // An optimistic read is possible if the correct stream is present
        // and that stream has no updates.
        long checkAddr = Address.MAX;
        long ts = 0;
        if (checkStreamUnsafe()) {
            ts = lock.tryOptimisticRead();
            if (ts != 0) {
                checkAddr = smrStream.check();
                if (checkAddr == Address.UP_TO_DATE
                        || checkAddr == smrStream.pos()) {
                    try {
                        // Up to date, don't need to sync
                        R ret = accessFunction.access(object);
                        if (lock.validate(ts)) {
                            return ret;
                        }
                    } catch (Exception ex) {
                        // An exceptional condition occurred during the optimistic read
                        // We catch the exception but don't throw it, since it may have
                        // been a result of inconsistent state.
                    }
                }

            }
        }
        final long syncAddress = checkAddr == Address.UP_TO_DATE ? Address.MAX : checkAddr;
        try {
            ts = lock.writeLock();
            checkAndSwitchStreamsUnsafe();
            syncObjectUnsafe(syncAddress, conflictObject);
            return accessFunction.access(object);
        } finally {
            lock.unlock(ts);
        }
    }

    protected void rollbackToParent() {
        while (smrStream.getParent() != null) {
            syncObjectUnsafe(Address.OPTIMISTIC, null);
            smrStream = smrStream.getParent();
        }
    }

    /**
     * Get the version of this object. This corresponds to the position
     * of the pointer into the SMR stream.
     *
     * @return Returns the pointer position to the object in the stream.
     */
    public long getVersionUnsafe() {
        return smrStream.pos();
    }

    /**
     * Reset this object to the uninitialized state.
     */
    public void resetUnsafe() {
        log.debug("Reset[{}]", this);
        object = builder.getRawInstance();
        rollbackToParent();
        smrStream.reset();
    }

    @Override
    public int hashCode() {
        return builder.getStreamId().hashCode();
    }

    /**
     * Generate the summary string for this version locked object.
     *
     * <p>The format of this string is [type]@[version][+]
     * (where + is the optimistic flag)
     *
     * @return The summary string for this version locked object
     */
    @Override
    public String toString() {
        return object.getClass().getSimpleName()
                + "[" + Utils.toReadableId(smrStream.getId()) + "]@"
                + (getVersionUnsafe() == Address.NEVER_READ ? "NR" : getVersionUnsafe());
    }


    /**
     * Sync this stream by playing updates forward in the stream until
     * the given timestamp. If Address.MAX is given, updates will be
     * applied until the current tail of the stream. If Address.OPTIMISTIC
     * is given, updates will be applied to the end of the stream, and
     * upcall results will be stored in the resulting entries.
     *
     * <p>When the stream is trimmed, this exception is passed up to the caller,
     * unless the timestamp was Address.MAX, in which the entire object is
     * reset and re-try the sync, which should pick up any checkpoint that
     * was inserted.
     *
     * @param timestamp The timestamp to sync up to.
     */
    protected void syncObjectUnsafe(long timestamp, Object[] conflictObjects) {
        log.trace("Sync[{}] to {}", this, timestamp);
        for (int tries = 0; tries < builder.getRuntime().trimRetry; tries++) {
            try {
                smrStream.sync(timestamp, conflictObjects)
                    .forEachOrdered(op -> {
                        try {
                            object = op.apply(wrapper, object);
                        } catch (Exception e) {
                            log.error("Sync[{}] Error: Couldn't execute upcall due to {}", this, e);
                            throw new RuntimeException(e);
                        }
                    });
                return;
            } catch (NoRollbackException nre) {
                log.info("Failed to rollback, resetting and retrying");
                resetUnsafe();
            } catch (TrimmedException te) {
                if (!te.isRetriable()) {
                    log.warn("Trim is not retriable, aborting sync");
                    throw te;
                } else if (tries + 1 >= builder.getRuntime().trimRetry) {
                    if (Transactions.active()) {
                        Transactions.abort(new TransactionAbortedException(
                                new TxResolutionInfo(
                                       Transactions.current().getTransactionID(),
                                       Transactions.getReadSnapshot()), null, builder.getStreamId(),
                                AbortCause.TRIM, te, Transactions.current()));
                    } else {
                        throw te;
                    }
                }
                log.info("Encountered trim during playback, resetting and retrying");
                resetUnsafe();
            }

            // We must restore the transactional stream if we were operating transactionally
            checkAndSwitchStreamsUnsafe();
        }
        throw new RuntimeException("Unable to sync object and retry limit exhausted");
    }

    protected void checkAndSwitchStreamsUnsafe() {
        if (Transactions.active()) {
            IStateMachineStream txStream = Transactions.current()
                    .getStateMachineStream(this, smrStream);
            if (!txStream.equals(smrStream)) {
                rollbackToParent();
                smrStream = txStream;
            }
        } else if (smrStream.getParent() != null) {
            rollbackToParent();
        }
    }

    protected boolean checkStreamUnsafe() {
        if (Transactions.active()) {
            return Transactions.current().getStateMachineStream(this, smrStream)
                    .equals(smrStream);
        } else {
            return smrStream.getParent() == null;
        }
    }

    @Nonnull
    public IStateMachineStream installNewStream(@Nonnull
                            Function<IStateMachineStream, IStateMachineStream> streamGenerator) {
        long ts = lock.writeLock();
        try {
            rollbackToParent();
            smrStream = streamGenerator.apply(smrStream.getRoot());
            return smrStream;
        } finally {
            lock.unlock(ts);
        }
    }

    @Override
    public long getVersion() {
        return getVersionUnsafe();
    }

    @Override
    public UUID getId() {
        return builder.getStreamId();
    }

    void abortTransaction(Exception e) {
        long snapshotTimestamp;
        AbortCause abortCause;
        TransactionAbortedException tae;

        AbstractTransaction context = Transactions.current();

        if (e instanceof TransactionAbortedException) {
            tae = (TransactionAbortedException) e;
        } else {
            if (e instanceof NetworkException) {
                // If a 'NetworkException' was received within a transactional context, an attempt to
                // 'getSnapshotTimestamp' will also fail (as it requests it to the Sequencer).
                // A new NetworkException would prevent the earliest to be propagated and encapsulated
                // as a TransactionAbortedException.
                snapshotTimestamp = -1L;
                abortCause = AbortCause.NETWORK;
            } else if (e instanceof UnsupportedOperationException) {
                snapshotTimestamp = Transactions.getReadSnapshot();
                abortCause = AbortCause.UNSUPPORTED;
            } else {
                log.error("abort[{}] Abort Transaction with Exception {}", this, e);
                snapshotTimestamp = Transactions.getReadSnapshot();
                abortCause = AbortCause.UNDEFINED;
            }

            TxResolutionInfo txInfo = new TxResolutionInfo(
                    context.getTransactionID(), snapshotTimestamp);
            // TODO: fix...
            tae = new TransactionAbortedException(txInfo, null, UUID.randomUUID(),
                    abortCause, e, context);
            context.abort(tae);
        }


        // Discard the transaction chain, if present.
        try {
            Transactions.abort();
        } catch (TransactionAbortedException e2) {
            // discard manual abort
        }

        throw tae;
    }

    @Override
    public <R> R txExecute(Supplier<R> txFunction) {
        // Don't nest transactions if we are already running transactionally
        if (Transactions.active()) {
            try {
                return txFunction.get();
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                abortTransaction(e);
            }
        }
        long sleepTime = 1L;
        final long maxSleepTime = 1000L;
        int retries = 1;
        while (true) {
            try {
                builder.getRuntime().getObjectsView().TXBegin();
                R ret = txFunction.get();
                builder.getRuntime().getObjectsView().TXEnd();
                return ret;
            } catch (TransactionAbortedException e) {
                // If TransactionAbortedException is due to a 'Network Exception' do not keep
                // retrying a nested transaction indefinitely (this could go on forever).
                // If this is part of an outer transaction abort and remove from context.
                // Re-throw exception to client.
                log.warn("TXExecute[{}] Abort with exception {}", this, e);
                if (e.getAbortCause() == AbortCause.NETWORK) {
                    if (Transactions.current() != null) {
                        try {
                            Transactions.abort();
                        } catch (TransactionAbortedException tae) {
                            // discard manual abort
                        }
                        throw e;
                    }
                }

                log.debug("Transactional function aborted due to {}, retrying after {} msec",
                        e, sleepTime);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    log.warn("TxExecuteInner retry sleep interrupted {}", ie);
                }
                sleepTime = min(sleepTime * 2L, maxSleepTime);
                retries++;
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                abortTransaction(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void sync() {
        long ts = lock.writeLock();
        try {
            checkAndSwitchStreamsUnsafe();
            syncObjectUnsafe(Address.MAX, null);
        } finally {
            lock.unlock(ts);
        }
    }


    private @Nonnull IStateMachineStream getActiveStream() {
        return Transactions.active()
                ? Transactions.current().getStateMachineStream(this, smrStream.getRoot())
                : smrStream.getRoot();
    }
}
