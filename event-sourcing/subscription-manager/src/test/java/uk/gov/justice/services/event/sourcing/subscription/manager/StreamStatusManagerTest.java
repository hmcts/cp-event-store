package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static javax.transaction.Status.STATUS_ACTIVE;
import static javax.transaction.Status.STATUS_NO_TRANSACTION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.subscription.NewStreamStatusRepository;
import uk.gov.justice.services.event.buffer.core.repository.subscription.StreamStatusException;
import uk.gov.justice.services.event.buffer.core.repository.subscription.TransactionException;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.transaction.SystemException;
import javax.transaction.UserTransaction;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StreamStatusManagerTest {

    @Mock
    private UserTransaction userTransaction;

    @Mock
    private NewStreamStatusRepository newStreamStatusRepository;

    @InjectMocks
    private StreamStatusManager streamStatusManager;

    @Test
    public void shouldCreateNewStreamStatusInItsOwnTransactionAndCommit() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final ZonedDateTime updatedAt = new UtcClock().now();
        final boolean isUpToDate = true;

        streamStatusManager.createNewStreamInStreamStatusTableIfNecessary(
                streamId,
                source,
                componentName,
                updatedAt,
                isUpToDate
        );

        final InOrder inOrder = inOrder(userTransaction, newStreamStatusRepository);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                isUpToDate
        );
        inOrder.verify(userTransaction).commit();
    }

    @Test
    public void shouldRollBackTransactionIfUpdateFails() throws Exception {

        final UUID streamId = fromString("acffa684-5d58-43fc-b34d-1d74948f237a");
        final String source = "some-source";
        final String componentName = "some-component";
        final ZonedDateTime updatedAt = new UtcClock().now();
        final boolean isUpToDate = true;
        final NullPointerException nullPointerException = new NullPointerException("Curses curses");

        doThrow(nullPointerException).when(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                isUpToDate
        );
        when(userTransaction.getStatus()).thenReturn(STATUS_ACTIVE);

        final StreamStatusException streamStatusException = assertThrows(StreamStatusException.class,
                () -> streamStatusManager.createNewStreamInStreamStatusTableIfNecessary(
                        streamId,
                        source,
                        componentName,
                        updatedAt,
                        isUpToDate)
        );

        assertThat(streamStatusException.getCause(), is(nullPointerException));
        assertThat(streamStatusException.getMessage(), is("Failed to insert stream into stream_status table: streamId 'acffa684-5d58-43fc-b34d-1d74948f237a' source 'some-source' component 'some-component'"));

        final InOrder inOrder = inOrder(userTransaction, newStreamStatusRepository);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                isUpToDate
        );
        inOrder.verify(userTransaction).rollback();

        verify(userTransaction, never()).commit();
    }

    @Test
    public void shouldNotRollBackOnFailureIfTransactionNotActive() throws Exception {

        final UUID streamId = fromString("acffa684-5d58-43fc-b34d-1d74948f237a");
        final String source = "some-source";
        final String componentName = "some-component";
        final ZonedDateTime updatedAt = new UtcClock().now();
        final boolean isUpToDate = true;
        final NullPointerException nullPointerException = new NullPointerException("Curses curses");

        doThrow(nullPointerException).when(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                isUpToDate
        );
        when(userTransaction.getStatus()).thenReturn(STATUS_NO_TRANSACTION);

        final StreamStatusException streamStatusException = assertThrows(StreamStatusException.class,
                () -> streamStatusManager.createNewStreamInStreamStatusTableIfNecessary(
                        streamId,
                        source,
                        componentName,
                        updatedAt,
                        isUpToDate)
        );

        assertThat(streamStatusException.getCause(), is(nullPointerException));
        assertThat(streamStatusException.getMessage(), is("Failed to insert stream into stream_status table: streamId 'acffa684-5d58-43fc-b34d-1d74948f237a' source 'some-source' component 'some-component'"));

        final InOrder inOrder = inOrder(userTransaction, newStreamStatusRepository);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                isUpToDate
        );

        verify(userTransaction, never()).rollback();
        verify(userTransaction, never()).commit();
    }

    @Test
    public void shouldThrowTransactionExceptionIfRollbackFails() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final ZonedDateTime updatedAt = new UtcClock().now();
        final boolean isUpToDate = true;
        final NullPointerException nullPointerException = new NullPointerException("Curses curses");
        final SystemException systemException = new SystemException("Oh no. Rollback failed");

        doThrow(nullPointerException).when(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                isUpToDate
        );
        when(userTransaction.getStatus()).thenReturn(STATUS_ACTIVE);
        doThrow(systemException).when(userTransaction).rollback();

        final TransactionException transactionException = assertThrows(TransactionException.class,
                () -> streamStatusManager.createNewStreamInStreamStatusTableIfNecessary(
                        streamId,
                        source,
                        componentName,
                        updatedAt,
                        isUpToDate)
        );

        assertThat(transactionException.getCause(), is(systemException));
        assertThat(transactionException.getMessage(), is("Unexpected exception during transaction rollback, rollback maybe incomplete"));

        final InOrder inOrder = inOrder(userTransaction, newStreamStatusRepository);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(newStreamStatusRepository).insertIfNotExists(
                streamId,
                source,
                componentName,
                updatedAt,
                isUpToDate
        );
        inOrder.verify(userTransaction).rollback();

        verify(userTransaction, never()).commit();
    }
}