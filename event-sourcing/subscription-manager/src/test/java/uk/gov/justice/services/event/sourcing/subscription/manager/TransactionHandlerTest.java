package uk.gov.justice.services.event.sourcing.subscription.manager;

import static javax.transaction.Status.STATUS_ACTIVE;
import static javax.transaction.Status.STATUS_MARKED_ROLLBACK;
import static javax.transaction.Status.STATUS_NO_TRANSACTION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.subscription.TransactionException;
import uk.gov.justice.services.event.sourcing.subscription.manager.TransactionHandler.SavepointContext;
import uk.gov.justice.services.jdbc.persistence.ViewStoreJdbcDataSourceProvider;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;

import javax.persistence.EntityManager;
import javax.sql.DataSource;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class TransactionHandlerTest {

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private ViewStoreJdbcDataSourceProvider viewStoreJdbcDataSourceProvider;

    @Mock
    private EntityManager entityManager;

    @Mock
    private Logger logger;

    @InjectMocks
    private TransactionHandler transactionHandler;

    @Test
    public void shouldBeginUserTransaction() throws Exception {

        transactionHandler.begin();

        verify(userTransaction).begin();
    }

    @Test
    public void shouldThrowTransactionExceptionIfBeginUserTransactionThrowsSystemException() throws Exception {

        final SystemException systemException = new SystemException();

        doThrow(systemException).when(userTransaction).begin();

        final TransactionException transactionException = assertThrows(TransactionException.class, () -> transactionHandler.begin());

        assertThat(transactionException.getCause(), is(systemException));
        assertThat(transactionException.getMessage(), is("Failed to begin UserTransaction"));
    }

    @Test
    public void shouldThrowTransactionExceptionIfBeginUserTransactionThrowsNotSupportedException() throws Exception {

        final NotSupportedException notSupportedException = new NotSupportedException();

        doThrow(notSupportedException).when(userTransaction).begin();

        final TransactionException transactionException = assertThrows(TransactionException.class, () -> transactionHandler.begin());

        assertThat(transactionException.getCause(), is(notSupportedException));
        assertThat(transactionException.getMessage(), is("Failed to begin UserTransaction"));
    }

    @Test
    public void shouldCommitUserTransaction() throws Exception {

        transactionHandler.commit();

        verify(userTransaction).commit();
    }

    @Test
    public void shouldThrowTransactionExceptionIfCommitUserTransactionThrowsSystemException() throws Exception {

        final SystemException systemException = new SystemException();

        doThrow(systemException).when(userTransaction).commit();

        final TransactionException transactionException = assertThrows(TransactionException.class, () -> transactionHandler.commit());

        assertThat(transactionException.getCause(), is(systemException));
        assertThat(transactionException.getMessage(), is("Failed to commit UserTransaction"));
    }

    @Test
    public void shouldThrowTransactionExceptionIfCommitUserTransactionThrowsRollbackException() throws Exception {

        final RollbackException rollbackException = new RollbackException();

        doThrow(rollbackException).when(userTransaction).commit();

        final TransactionException transactionException = assertThrows(TransactionException.class, () -> transactionHandler.commit());

        assertThat(transactionException.getCause(), is(rollbackException));
        assertThat(transactionException.getMessage(), is("Failed to commit UserTransaction"));
    }

    @Test
    public void shouldThrowTransactionExceptionIfCommitUserTransactionThrowsHeuristicMixedException() throws Exception {

        final HeuristicMixedException heuristicMixedException = new HeuristicMixedException();

        doThrow(heuristicMixedException).when(userTransaction).commit();

        final TransactionException transactionException = assertThrows(TransactionException.class, () -> transactionHandler.commit());

        assertThat(transactionException.getCause(), is(heuristicMixedException));
        assertThat(transactionException.getMessage(), is("Failed to commit UserTransaction"));
    }

    @Test
    public void shouldThrowTransactionExceptionIfCommitUserTransactionThrowsHeuristicRollbackException() throws Exception {

        final HeuristicRollbackException heuristicRollbackException = new HeuristicRollbackException();

        doThrow(heuristicRollbackException).when(userTransaction).commit();

        final TransactionException transactionException = assertThrows(TransactionException.class, () -> transactionHandler.commit());

        assertThat(transactionException.getCause(), is(heuristicRollbackException));
        assertThat(transactionException.getMessage(), is("Failed to commit UserTransaction"));
    }

    @Test
    public void shouldRollBackUserTransaction() throws Exception {

        when(userTransaction.getStatus()).thenReturn(STATUS_ACTIVE);

        transactionHandler.rollback();

        verify(userTransaction).rollback();
    }

    @Test
    public void shouldNotRollBackTransactionIfNoTransactionActive() throws Exception {

        when(userTransaction.getStatus()).thenReturn(STATUS_NO_TRANSACTION);

        transactionHandler.rollback();

        verify(userTransaction, never()).rollback();
        verifyNoInteractions(logger);
    }

    @Test
    public void shouldLogAndDoNothingIfRollbackTransactionThrowsSystemException() throws Exception {

        final SystemException systemException = new SystemException();

        doThrow(systemException).when(userTransaction).rollback();

        transactionHandler.rollback();

        verify(logger).error("Failed to rollback transaction, rollback maybe incomplete", systemException);
    }

    @Test
    public void shouldLogAndDoNothingIfRollbackTransactionThrowsIllegalStateException() throws Exception {

        final IllegalStateException illegalStateException = new IllegalStateException();

        doThrow(illegalStateException).when(userTransaction).rollback();

        transactionHandler.rollback();

        verify(logger).error("Failed to rollback transaction, rollback maybe incomplete", illegalStateException);
    }

    @Test
    public void shouldCreateSavepointContext() throws Exception {

        final DataSource dataSource = mock(DataSource.class);
        final Connection viewStoreConnection = mock(Connection.class);
        final Connection physicalConnection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);

        when(viewStoreJdbcDataSourceProvider.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(viewStoreConnection);
        when(viewStoreConnection.unwrap(any())).thenReturn(physicalConnection);
        when(physicalConnection.setSavepoint()).thenReturn(savepoint);

        final SavepointContext ctx = transactionHandler.createSavepointContext();

        assertThat(ctx.viewStoreConnection(), is(viewStoreConnection));
        assertThat(ctx.physicalConnection(), is(physicalConnection));
        assertThat(ctx.savepoint(), is(savepoint));
    }

    @Test
    public void shouldReleaseSavepoint() throws Exception {

        final Connection physicalConnection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);
        final SavepointContext ctx = new SavepointContext(mock(Connection.class), physicalConnection, savepoint);

        transactionHandler.releaseSavepoint(ctx);

        verify(physicalConnection).releaseSavepoint(savepoint);
    }

    @Test
    public void shouldRollbackSavepointAndClearEntityManager() throws Exception {

        final Connection physicalConnection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);
        final SavepointContext ctx = new SavepointContext(mock(Connection.class), physicalConnection, savepoint);

        transactionHandler.rollbackSavepoint(ctx);

        verify(physicalConnection).rollback(savepoint);
        verify(entityManager).clear();
    }

    @Test
    public void shouldRollbackTransactionAndThrowIfTainted() throws Exception {

        final Connection physicalConnection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);
        final SavepointContext ctx = new SavepointContext(mock(Connection.class), physicalConnection, savepoint);

        when(userTransaction.getStatus()).thenReturn(STATUS_MARKED_ROLLBACK);

        final TransactionTaintedException exception = assertThrows(
                TransactionTaintedException.class,
                () -> transactionHandler.checkTaintedAndRollbackToSavepoint(ctx));

        assertThat(exception.getMessage(), is(
                "JTA transaction unexpectedly marked for rollback. " +
                "EntityManagerFlushInterceptor should prevent transaction tainting via direct FlushEventListener.onFlush(). " +
                "This indicates a bug — row lock on stream_status has been lost."));
        verify(userTransaction).rollback();
        verify(physicalConnection, never()).rollback(savepoint);
    }

    @Test
    public void shouldRollbackSavepointWhenNotTainted() throws Exception {

        final Connection physicalConnection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);
        final SavepointContext ctx = new SavepointContext(mock(Connection.class), physicalConnection, savepoint);

        when(userTransaction.getStatus()).thenReturn(STATUS_ACTIVE);

        transactionHandler.checkTaintedAndRollbackToSavepoint(ctx);

        verify(physicalConnection).rollback(savepoint);
        verify(entityManager).clear();
        verify(userTransaction, never()).rollback();
    }

    @Test
    public void shouldWarnIfSavepointRollbackFailsWhenNotTainted() throws Exception {

        final Connection physicalConnection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);
        final SavepointContext ctx = new SavepointContext(mock(Connection.class), physicalConnection, savepoint);
        final SQLException sqlException = new SQLException("Rollback failed");

        when(userTransaction.getStatus()).thenReturn(STATUS_ACTIVE);
        doThrow(sqlException).when(physicalConnection).rollback(savepoint);

        transactionHandler.checkTaintedAndRollbackToSavepoint(ctx);

        verify(logger).warn("Failed to rollback savepoint", sqlException);
        verify(userTransaction, never()).rollback();
    }

    @Test
    public void shouldReleaseSavepointAndCommit() throws Exception {

        final Connection physicalConnection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);
        final SavepointContext ctx = new SavepointContext(mock(Connection.class), physicalConnection, savepoint);

        transactionHandler.releaseSavepointAndCommit(ctx);

        verify(physicalConnection).releaseSavepoint(savepoint);
        verify(userTransaction).commit();
    }

    @Test
    public void shouldCommitEvenIfReleaseSavepointFails() throws Exception {

        final Connection physicalConnection = mock(Connection.class);
        final Savepoint savepoint = mock(Savepoint.class);
        final SavepointContext ctx = new SavepointContext(mock(Connection.class), physicalConnection, savepoint);
        final SQLException sqlException = new SQLException("Release failed");

        doThrow(sqlException).when(physicalConnection).releaseSavepoint(savepoint);

        transactionHandler.releaseSavepointAndCommit(ctx);

        verify(logger).warn("Failed to release savepoint, continuing with commit", sqlException);
        verify(userTransaction).commit();
    }

    @Test
    public void shouldCommitWithFallbackToRollback() throws Exception {

        transactionHandler.commitWithFallbackToRollback();

        verify(userTransaction).commit();
        verify(userTransaction, never()).rollback();
    }

    @Test
    public void shouldRollbackWhenCommitFailsInFallback() throws Exception {

        doThrow(new TransactionException("Failed to commit UserTransaction", new SystemException())).when(userTransaction).commit();
        when(userTransaction.getStatus()).thenReturn(STATUS_ACTIVE);

        transactionHandler.commitWithFallbackToRollback();

        verify(userTransaction).rollback();
    }

    @Test
    public void shouldCloseSavepointContextViaHandler() throws Exception {

        final Connection viewStoreConnection = mock(Connection.class);
        final SavepointContext ctx = new SavepointContext(viewStoreConnection, mock(Connection.class), mock(Savepoint.class));

        transactionHandler.closeSavepointContext(ctx);

        verify(viewStoreConnection).close();
    }

    @Test
    public void shouldLogWarningWhenCloseSavepointContextFails() throws Exception {

        final Connection viewStoreConnection = mock(Connection.class);
        final SavepointContext ctx = new SavepointContext(viewStoreConnection, mock(Connection.class), mock(Savepoint.class));
        final SQLException sqlException = new SQLException("Close failed");

        doThrow(sqlException).when(viewStoreConnection).close();

        transactionHandler.closeSavepointContext(ctx);

        verify(logger).warn("Failed to close savepoint context connection", sqlException);
    }

    @Test
    public void shouldCloseSavepointContextConnectionDirectly() throws Exception {

        final Connection viewStoreConnection = mock(Connection.class);
        final SavepointContext ctx = new SavepointContext(viewStoreConnection, mock(Connection.class), mock(Savepoint.class));

        ctx.close();

        verify(viewStoreConnection).close();
    }
}