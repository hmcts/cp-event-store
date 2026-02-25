package uk.gov.justice.services.event.sourcing.subscription.manager;

import static javax.transaction.Status.STATUS_MARKED_ROLLBACK;
import static javax.transaction.Status.STATUS_NO_TRANSACTION;

import uk.gov.justice.services.event.buffer.core.repository.subscription.TransactionException;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.slf4j.Logger;

public class TransactionHandler {

    @Inject
    private UserTransaction userTransaction;

    @Inject
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Inject
    private EntityManager entityManager;

    @Inject
    private Logger logger;

    public record SavepointContext(
            Connection viewStoreConnection,
            Connection physicalConnection,
            Savepoint savepoint
    ) implements AutoCloseable {
        @Override
        public void close() throws SQLException {
            viewStoreConnection.close();
        }
    }

    public void begin() {
        try {
            userTransaction.begin();
        } catch (final SystemException | NotSupportedException e) {
            throw new TransactionException("Failed to begin UserTransaction", e);
        }
    }

    public void commit() {
        try {
            userTransaction.commit();
        } catch (final SystemException | RollbackException | HeuristicMixedException | HeuristicRollbackException e) {
            throw new TransactionException("Failed to commit UserTransaction", e);
        }
    }

    public void rollback() {
        try {

            if (userTransaction.getStatus() != STATUS_NO_TRANSACTION) {
                userTransaction.rollback();
            }
        } catch (final SystemException | IllegalStateException e) {
            logger.error("Failed to rollback transaction, rollback maybe incomplete", e);
        }
    }

    public SavepointContext createSavepointContext() throws SQLException {
        final Connection viewStoreConnection = viewStoreJdbcDataSourceProvider.getDataSource().getConnection();
        final Connection physicalConnection = unwrapPhysicalConnection(viewStoreConnection);
        final Savepoint savepoint = physicalConnection.setSavepoint();
        return new SavepointContext(viewStoreConnection, physicalConnection, savepoint);
    }

    public void releaseSavepoint(final SavepointContext ctx) throws SQLException {
        ctx.physicalConnection().releaseSavepoint(ctx.savepoint());
    }

    public void rollbackSavepoint(final SavepointContext ctx) throws SQLException {
        ctx.physicalConnection().rollback(ctx.savepoint());
        entityManager.clear();
    }

    public void releaseSavepointAndCommit(final SavepointContext ctx) {
        try {
            releaseSavepoint(ctx);
        } catch (final SQLException e) {
            logger.warn("Failed to release savepoint, continuing with commit", e);
        }
        commit();
    }

    public void rollbackSavepointAndRestartIfTainted(final SavepointContext ctx) {
        try {
            rollbackSavepoint(ctx);
        } catch (final SQLException e) {
            logger.warn("Failed to rollback savepoint, continuing with transaction recovery", e);
        }
        restartTransactionIfTainted();
    }

    public void closeSavepointContext(final SavepointContext ctx) {
        try {
            ctx.close();
        } catch (final SQLException e) {
            logger.warn("Failed to close savepoint context connection", e);
        }
    }

    public void restartTransactionIfTainted() {
        if (isTransactionMarkedForRollback()) {
            rollback();
            begin();
        }
    }

    public void commitWithFallbackToRollback() {
        try {
            commit();
        } catch (final Exception e) {
            rollback();
        }
    }

    private boolean isTransactionMarkedForRollback() {
        try {
            return userTransaction.getStatus() == STATUS_MARKED_ROLLBACK;
        } catch (final SystemException e) {
            logger.warn("Failed to check transaction status", e);
            return false;
        }
    }

    private Connection unwrapPhysicalConnection(final Connection connection) throws SQLException {
        try {
            return (Connection) connection.unwrap((Class<?>) Class.forName("org.postgresql.PGConnection"));
        } catch (final ClassNotFoundException e) {
            return connection;
        }
    }
}
