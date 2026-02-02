package uk.gov.justice.services.eventsourcing.eventpublishing;

import java.util.UUID;
import javax.transaction.UserTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberLinkingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.eventpublishing.EventNumberLinker.ADVISORY_LOCK_KEY;

@ExtendWith(MockitoExtension.class)
public class EventNumberLinkerTest {
    @Mock
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Mock
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Mock
    private AdvisoryLockDataAccess advisoryLockDataAccess;

    @Mock
    private UserTransaction userTransaction;

    @InjectMocks
    private EventNumberLinker eventNumberLinker;

    @Test
    public void shouldFindAndLinkNextAvailableUnlinkedEventAndLinkInEventLogTable() throws Exception {

        final Long previousEventNumber = 22L;
        final Long newEventNumber = previousEventNumber + 1;
        final UUID eventId = randomUUID();
        final int transactionTimeoutSeconds = 300;
        final int localStatementTimeoutSeconds = 10;

        when(eventLinkingWorkerConfig.getTransactionTimeoutSeconds()).thenReturn(transactionTimeoutSeconds);
        when(eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds()).thenReturn(localStatementTimeoutSeconds);
        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY)).thenReturn(true);
        when(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable()).thenReturn(previousEventNumber);
        when(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink()).thenReturn(of(eventId));

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(true));

        final InOrder inOrder = inOrder(userTransaction, eventLinkingWorkerConfig, linkEventsInEventLogDatabaseAccess, advisoryLockDataAccess);
        inOrder.verify(eventLinkingWorkerConfig).getTransactionTimeoutSeconds();
        inOrder.verify(eventLinkingWorkerConfig).getLocalStatementTimeoutSeconds();
        inOrder.verify(userTransaction).setTransactionTimeout(transactionTimeoutSeconds);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(linkEventsInEventLogDatabaseAccess).setStatementTimeoutOnCurrentTransaction(localStatementTimeoutSeconds);
        inOrder.verify(advisoryLockDataAccess).tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).linkEvent(eventId, newEventNumber, previousEventNumber);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).insertLinkedEventIntoPublishQueue(eventId);
        inOrder.verify(userTransaction).commit();

    }

    @Test
    public void shouldDoNothingIfAdvisoryLockNotAvailable() throws Exception {

        final int transactionTimeoutSeconds = 300;
        final int statementTimeoutSeconds = 10;

        when(eventLinkingWorkerConfig.getTransactionTimeoutSeconds()).thenReturn(transactionTimeoutSeconds);
        when(eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds()).thenReturn(statementTimeoutSeconds);
        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY)).thenReturn(false);

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(false));

        final InOrder inOrder = inOrder(userTransaction, eventLinkingWorkerConfig, advisoryLockDataAccess, linkEventsInEventLogDatabaseAccess);
        inOrder.verify(eventLinkingWorkerConfig).getTransactionTimeoutSeconds();
        inOrder.verify(userTransaction).setTransactionTimeout(transactionTimeoutSeconds);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(linkEventsInEventLogDatabaseAccess).setStatementTimeoutOnCurrentTransaction(statementTimeoutSeconds);
        inOrder.verify(advisoryLockDataAccess).tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY);
        inOrder.verify(userTransaction).commit();

        verifyNoMoreInteractions(linkEventsInEventLogDatabaseAccess);
    }

    @Test
    public void shouldDoNothingIfNoUnlinkedEventsFound() throws Exception {

        final int transactionTimeoutSeconds = 300;
        final int statementTimeoutSeconds = 10;

        when(eventLinkingWorkerConfig.getTransactionTimeoutSeconds()).thenReturn(transactionTimeoutSeconds);
        when(eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds()).thenReturn(statementTimeoutSeconds);
        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY)).thenReturn(true);
        when(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink()).thenReturn(empty());

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(false));

        final InOrder inOrder = inOrder(userTransaction, eventLinkingWorkerConfig, advisoryLockDataAccess, linkEventsInEventLogDatabaseAccess);
        inOrder.verify(eventLinkingWorkerConfig).getTransactionTimeoutSeconds();
        inOrder.verify(userTransaction).setTransactionTimeout(transactionTimeoutSeconds);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(linkEventsInEventLogDatabaseAccess).setStatementTimeoutOnCurrentTransaction(statementTimeoutSeconds);
        inOrder.verify(advisoryLockDataAccess).tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).findIdOfNextEventToLink();
        inOrder.verify(userTransaction).commit();

        verifyNoMoreInteractions(linkEventsInEventLogDatabaseAccess);
    }

    @Test
    public void shouldRollbackTransactionOnException() throws Exception {

        final int transactionTimeoutSeconds = 300;
        final Exception testException = new RuntimeException("Test exception");

        when(eventLinkingWorkerConfig.getTransactionTimeoutSeconds()).thenReturn(transactionTimeoutSeconds);
        doThrow(testException).when(userTransaction).begin();

        assertThrows(EventNumberLinkingException.class, () -> eventNumberLinker.findAndAndLinkNextUnlinkedEvent());

        final InOrder inOrder = inOrder(userTransaction, eventLinkingWorkerConfig);
        inOrder.verify(eventLinkingWorkerConfig).getTransactionTimeoutSeconds();
        inOrder.verify(userTransaction).setTransactionTimeout(transactionTimeoutSeconds);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(userTransaction).rollback();
    }
}