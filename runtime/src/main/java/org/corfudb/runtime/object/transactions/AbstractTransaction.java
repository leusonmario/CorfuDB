package org.corfudb.runtime.object.transactions;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.object.VersionedObjectManager;
import org.corfudb.runtime.object.IStateMachineAccess;
import org.corfudb.runtime.object.IStateMachineEngine;
import org.corfudb.util.Utils;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a transactional context. Transactional contexts
 * manage per-thread transaction state.
 *
 * <p>Recall from {@link CorfuCompileProxy} that an SMR object layer implements objects whose
 * history of updates
 * are backed by a stream. If a Corfu object's method is an Accessor, it invokes the proxy's
 * access() method. Likewise, if a Corfu object's method is a Mutator or Accessor-Mutator,
 * it invokes the proxy's logUpdate() method.
 *
 * <p>Within transactional context, these methods invoke the transactionalContext
 * accessor/mutator helper.
 *
 * <p>For example, OptimisticTransaction.access() is responsible for
 * sync'ing the proxy state to the snapshot version, and then doing the access.
 *
 * <p>logUpdate() within transactional context is
 * responsible for updating the write-set.
 *
 * <p>Finally, if a Corfu object's method is an Accessor-Mutator, then although the mutation
 * is delayed, it needs to obtain the result by invoking getUpcallResult() on the optimistic
 * stream. This is similar to the second stage of access(), except working on the optimistic
 * stream instead of the underlying stream.
 *
 * <p>Created by mwei on 4/4/16.
 */
@Slf4j
@ToString
public abstract class AbstractTransaction {

    /**
     * Constant for a transaction which has been folded into
     * another transaction.
     */
    public static final long FOLDED_ADDRESS = -2L;

    /**
     * Constant for committing a transaction which did not
     * modify the log at all.
     */
    public static final long NOWRITE_ADDRESS = -4L;

    /**
     * The ID of the transaction. This is used for tracking only, it is
     * NOT recorded in the log.
     */
    @SuppressWarnings("checkstyle:abbreviationaswordinname")
    @Getter
    public UUID transactionID;

    /**
     * The builder used to create this transaction.
     */
    @Getter
    public final TransactionBuilder builder;

    @Getter
    public final AbstractTransaction parent;

    /**
     * The start time of the context.
     */
    @Getter
    public final long startTime;

    AbstractTransaction(TransactionBuilder builder, AbstractTransaction parent) {
        transactionID = UUID.randomUUID();
        startTime = System.currentTimeMillis();
        this.builder = builder;
        this.parent = parent;
    }


    public abstract IStateMachineStream getStateMachineStream(IObjectManager manager,
                                                              IStateMachineStream current);

    public long commit() throws TransactionAbortedException {
        return NOWRITE_ADDRESS;
    }

    /**
     * Forcefully abort the transaction.
     */
    public void abort(TransactionAbortedException ae) {
        AbstractTransaction.log.debug("abort[{}]", this);
    }

    @Override
    public String toString() {
        return "TX[" + Utils.toReadableId(transactionID) + "]";
    }
}