package uk.gov.justice.services.eventsourcing.eventpublishing;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import jakarta.transaction.UserTransaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventDetailsToLink;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberLinkingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkedEventData;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;

import static java.util.Collections.emptyList;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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

    @Mock
    private Connection connection;

    @Captor
    private ArgumentCaptor<List<LinkedEventData>> linkDataCaptor;

    @InjectMocks
    private EventNumberLinker eventNumberLinker;

    @Test
    public void shouldFindAndLinkBatchOfEventsUsingJdbcBatch() throws Exception {

        final UUID eventId1 = randomUUID();
        final UUID eventId2 = randomUUID();
        final UUID eventId3 = randomUUID();

        when(eventLinkingWorkerConfig.getBatchSize()).thenReturn(10);
        when(eventLinkingWorkerConfig.getTransactionTimeoutSeconds()).thenReturn(300);
        when(eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds()).thenReturn(10);
        when(linkEventsInEventLogDatabaseAccess.getEventStoreConnection()).thenReturn(connection);
        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, ADVISORY_LOCK_KEY)).thenReturn(true);
        final UUID streamId1 = randomUUID();
        final UUID streamId2 = randomUUID();
        final UUID streamId3 = randomUUID();
        final List<EventDetailsToLink> batch = List.of(
                new EventDetailsToLink(eventId1, streamId1, 1),
                new EventDetailsToLink(eventId2, streamId2, 1),
                new EventDetailsToLink(eventId3, streamId3, 1));

        when(linkEventsInEventLogDatabaseAccess.findBatchOfNextEventsToLink(connection, 10)).thenReturn(batch);
        when(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable(connection)).thenReturn(22L);

        final List<EventDetailsToLink> result = eventNumberLinker.findAndLinkEventsInBatch();
        assertThat(result.size(), is(3));

        final InOrder inOrder = inOrder(userTransaction, linkEventsInEventLogDatabaseAccess, advisoryLockDataAccess);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).getEventStoreConnection();
        inOrder.verify(userTransaction).setTransactionTimeout(300);
        inOrder.verify(userTransaction).begin();
        inOrder.verify(linkEventsInEventLogDatabaseAccess).setStatementTimeoutOnCurrentTransaction(connection, 10);
        inOrder.verify(advisoryLockDataAccess).tryNonBlockingTransactionLevelAdvisoryLock(connection, ADVISORY_LOCK_KEY);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).findBatchOfNextEventsToLink(connection, 10);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).findCurrentHighestEventNumberInEventLogTable(connection);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).linkEventsBatch(org.mockito.ArgumentMatchers.eq(connection), linkDataCaptor.capture());
        inOrder.verify(linkEventsInEventLogDatabaseAccess).insertBatchIntoPublishQueue(connection, List.of(eventId1, eventId2, eventId3));
        inOrder.verify(userTransaction).commit();

        final List<LinkedEventData> captured = linkDataCaptor.getValue();
        assertThat(captured.size(), is(3));
        assertThat(captured.get(0), is(new LinkedEventData(eventId1, 23L, 22L)));
        assertThat(captured.get(1), is(new LinkedEventData(eventId2, 24L, 23L)));
        assertThat(captured.get(2), is(new LinkedEventData(eventId3, 25L, 24L)));
    }

    @Test
    public void shouldReturnZeroIfAdvisoryLockNotAvailable() throws Exception {

        when(eventLinkingWorkerConfig.getBatchSize()).thenReturn(10);
        when(eventLinkingWorkerConfig.getTransactionTimeoutSeconds()).thenReturn(300);
        when(eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds()).thenReturn(10);
        when(linkEventsInEventLogDatabaseAccess.getEventStoreConnection()).thenReturn(connection);
        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, ADVISORY_LOCK_KEY)).thenReturn(false);

        assertThat(eventNumberLinker.findAndLinkEventsInBatch(), is(emptyList()));

        verify(userTransaction).rollback();
        verify(linkEventsInEventLogDatabaseAccess, never()).findBatchOfNextEventsToLink(connection, 10);
    }

    @Test
    public void shouldReturnEmptyIfNoUnlinkedEvents() throws Exception {

        when(eventLinkingWorkerConfig.getBatchSize()).thenReturn(10);
        when(eventLinkingWorkerConfig.getTransactionTimeoutSeconds()).thenReturn(300);
        when(eventLinkingWorkerConfig.getLocalStatementTimeoutSeconds()).thenReturn(10);
        when(linkEventsInEventLogDatabaseAccess.getEventStoreConnection()).thenReturn(connection);
        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(connection, ADVISORY_LOCK_KEY)).thenReturn(true);
        when(linkEventsInEventLogDatabaseAccess.findBatchOfNextEventsToLink(connection, 10)).thenReturn(emptyList());

        assertThat(eventNumberLinker.findAndLinkEventsInBatch(), is(emptyList()));

        verify(userTransaction).rollback();
        verify(linkEventsInEventLogDatabaseAccess, never()).findCurrentHighestEventNumberInEventLogTable(connection);
    }

    @Test
    public void shouldRollbackOnException() throws Exception {

        when(eventLinkingWorkerConfig.getBatchSize()).thenReturn(10);
        when(eventLinkingWorkerConfig.getTransactionTimeoutSeconds()).thenReturn(300);
        doThrow(new RuntimeException("Test")).when(userTransaction).begin();

        assertThrows(EventNumberLinkingException.class, () -> eventNumberLinker.findAndLinkEventsInBatch());

        verify(userTransaction).rollback();
    }
}