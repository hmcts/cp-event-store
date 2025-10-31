package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.eventpublishing.EventNumberLinker.ADVISORY_LOCK_KEY;

import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventLinkingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.AdvisoryLockDataAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.CompatibilityModePublishedEventRepository;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EventNumberLinkerTest {
    @Mock
    private EventLinkingWorkerConfig eventLinkingWorkerConfig;

    @Mock
    private CompatibilityModePublishedEventRepository compatibilityModePublishedEventRepository;

    @Mock
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Mock
    private AdvisoryLockDataAccess advisoryLockDataAccess;

    @InjectMocks
    private EventNumberLinker eventNumberLinker;

    @Test
    public void shouldFindAndLinkNextAvailableUnlinkedEventAndLinkInEventLogTable() throws Exception {

        final Long previousEventNumber = 22L;
        final Long newEventNumber = previousEventNumber + 1;
        final UUID eventId = randomUUID();

        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY)).thenReturn(true);
        when(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable()).thenReturn(previousEventNumber);
        when(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink()).thenReturn(of(eventId));
        when(eventLinkingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable()).thenReturn(true);

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(true));

        final InOrder inOrder = inOrder(linkEventsInEventLogDatabaseAccess, advisoryLockDataAccess, compatibilityModePublishedEventRepository);
        inOrder.verify(advisoryLockDataAccess).tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).linkEvent(eventId, newEventNumber, previousEventNumber);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).insertLinkedEventIntoPublishQueue(eventId);
        inOrder.verify(compatibilityModePublishedEventRepository).insertIntoPublishedEvent(eventId, newEventNumber, previousEventNumber);
        inOrder.verify(compatibilityModePublishedEventRepository).setEventNumberSequenceTo(newEventNumber);

    }

    @Test
    public void shouldDoNothingIfAdvisoryLockNotAvailable() throws Exception {

        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY)).thenReturn(false);

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(false));

        verifyNoInteractions(linkEventsInEventLogDatabaseAccess);
        verifyNoMoreInteractions(compatibilityModePublishedEventRepository);
    }

    @Test
    public void shouldDoNothingIfNoUnlinkedEventsFound() throws Exception {

        when(advisoryLockDataAccess.tryNonBlockingTransactionLevelAdvisoryLock(ADVISORY_LOCK_KEY)).thenReturn(true);
        when(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink()).thenReturn(empty());

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(false));

        verifyNoMoreInteractions(linkEventsInEventLogDatabaseAccess);
        verifyNoMoreInteractions(advisoryLockDataAccess);
        verifyNoMoreInteractions(compatibilityModePublishedEventRepository);
    }
}