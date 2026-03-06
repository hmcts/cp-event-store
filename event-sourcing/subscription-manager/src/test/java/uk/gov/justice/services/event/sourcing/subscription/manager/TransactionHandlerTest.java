package uk.gov.justice.services.event.sourcing.subscription.manager;

import static javax.transaction.Status.STATUS_ACTIVE;
import static javax.transaction.Status.STATUS_NO_TRANSACTION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.subscription.TransactionException;

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

        final TransactionException transactionException = assertThrows(
                TransactionException.class,
                () -> transactionHandler.begin());

        assertThat(transactionException.getCause(), is(systemException));
        assertThat(transactionException.getMessage(), is("Failed to begin UserTransaction"));
    }

    @Test
    public void shouldThrowTransactionExceptionIfBeginUserTransactionThrowsNotSupportedException() throws Exception {

        final NotSupportedException notSupportedException = new NotSupportedException();

        doThrow(notSupportedException).when(userTransaction).begin();

        final TransactionException transactionException = assertThrows(
                TransactionException.class,
                () -> transactionHandler.begin());

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

        final TransactionException transactionException = assertThrows(
                TransactionException.class,
                () -> transactionHandler.commit());

        assertThat(transactionException.getCause(), is(systemException));
        assertThat(transactionException.getMessage(), is("Failed to commit UserTransaction"));
    }

    @Test
    public void shouldThrowTransactionExceptionIfCommitUserTransactionThrowsRollbackException() throws Exception {

        final RollbackException rollbackException = new RollbackException();

        doThrow(rollbackException).when(userTransaction).commit();

        final TransactionException transactionException = assertThrows(
                TransactionException.class,
                () -> transactionHandler.commit());

        assertThat(transactionException.getCause(), is(rollbackException));
        assertThat(transactionException.getMessage(), is("Failed to commit UserTransaction"));
    }

    @Test
    public void shouldThrowTransactionExceptionIfCommitUserTransactionThrowsHeuristicMixedException() throws Exception {

        final HeuristicMixedException heuristicMixedException = new HeuristicMixedException();

        doThrow(heuristicMixedException).when(userTransaction).commit();

        final TransactionException transactionException = assertThrows(
                TransactionException.class,
                () -> transactionHandler.commit());

        assertThat(transactionException.getCause(), is(heuristicMixedException));
        assertThat(transactionException.getMessage(), is("Failed to commit UserTransaction"));
    }

    @Test
    public void shouldThrowTransactionExceptionIfCommitUserTransactionThrowsHeuristicRollbackException() throws Exception {

        final HeuristicRollbackException heuristicRollbackException = new HeuristicRollbackException();

        doThrow(heuristicRollbackException).when(userTransaction).commit();

        final TransactionException transactionException = assertThrows(
                TransactionException.class,
                () -> transactionHandler.commit());

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
}