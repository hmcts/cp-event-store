package uk.gov.justice.services.event.sourcing.subscription.manager;

import static javax.transaction.Status.STATUS_NO_TRANSACTION;

import uk.gov.justice.services.event.buffer.core.repository.subscription.TransactionException;

import java.util.UUID;

import javax.inject.Inject;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.slf4j.Logger;

public class TransactionHandler {

    @Inject
    private StreamSessionLockManager streamSessionLockManager;

    @Inject
    private Logger logger;

    public void begin(final UserTransaction userTransaction) {
        try {
            userTransaction.begin();
        } catch (final SystemException | NotSupportedException e) {
            throw new TransactionException("Failed to begin UserTransaction", e);
        }
    }

    public void commit(final UserTransaction userTransaction) {
        try {
            userTransaction.commit();
        } catch (final SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
            throw new TransactionException("Failed to commit UserTransaction", e);
        }
    }

    public void rollback(final UserTransaction userTransaction) {
        try {

            if (userTransaction.getStatus() != STATUS_NO_TRANSACTION) {
                userTransaction.rollback();
            }
        } catch (final SystemException | IllegalStateException e) {
            logger.error("Failed to rollback transaction, rollback maybe incomplete", e);
        }
    }

    public TransactionContext acquireConnection() {
        final var jtaHandle = streamSessionLockManager.getJtaConnection();
        final var physicalConnection = streamSessionLockManager.unwrapPhysicalConnection(jtaHandle);
        logger.trace("CONN-TRACE [acquireConnection] jtaHandle={}, physicalPID={}", System.identityHashCode(jtaHandle), getBackendPid(physicalConnection));
        return new TransactionContext(jtaHandle, physicalConnection);
    }

    private int getBackendPid(final java.sql.Connection connection) {
        try (final var ps = connection.prepareStatement("SELECT pg_backend_pid()");
             final var rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt(1) : -1;
        } catch (final java.sql.SQLException e) {
            return -1;
        }
    }

    public boolean tryLockStream(final TransactionContext transactionContext, final UUID streamId) {
        final boolean locked = streamSessionLockManager.tryLockStream(transactionContext.physicalConnection(), streamId);
        if (locked) {
            transactionContext.setLockedStreamId(streamId);
        }
        return locked;
    }

    public void releaseContext(final TransactionContext transactionContext) {
        if (transactionContext == null) {
            return;
        }
        if (transactionContext.lockedStreamId() != null) {
            streamSessionLockManager.unlockStream(transactionContext.physicalConnection(), transactionContext.lockedStreamId());
        }
        streamSessionLockManager.closeHandle(transactionContext.jtaHandle());
    }
}
