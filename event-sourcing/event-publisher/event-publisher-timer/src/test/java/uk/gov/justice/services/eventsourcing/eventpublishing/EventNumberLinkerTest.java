package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventNumberSequenceDataAccess;
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
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Mock
    private EventNumberSequenceDataAccess eventNumberSequenceDataAccess;

    @InjectMocks
    private EventNumberLinker eventNumberLinker;

    @Test
    public void shouldFindAndLinkNextAvailableUnlinkedEventAndLinkInEventLogTable() throws Exception {

        final Long newEventNumber = 23L;
        final Long previousEventNumber = 22L;
        final UUID eventId = randomUUID();

        when(eventNumberSequenceDataAccess.lockAndGetNextAvailableEventNumber()).thenReturn(newEventNumber);
        when(linkEventsInEventLogDatabaseAccess.findCurrentHighestEventNumberInEventLogTable()).thenReturn(previousEventNumber);
        when(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink()).thenReturn(of(eventId));

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(true));

        final InOrder inOrder = inOrder(linkEventsInEventLogDatabaseAccess, eventNumberSequenceDataAccess);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).linkEvent(eventId, newEventNumber, previousEventNumber);
        inOrder.verify(eventNumberSequenceDataAccess).updateNextAvailableEventNumberTo(newEventNumber + 1);
        inOrder.verify(linkEventsInEventLogDatabaseAccess).insertLinkedEventIntoPublishQueue(eventId);
    }

    @Test
    public void shouldDoNothingIfNoUnlinkedEventsFound() throws Exception {

        final Long newEventNumber = 23L;
        
        when(eventNumberSequenceDataAccess.lockAndGetNextAvailableEventNumber()).thenReturn(newEventNumber);
        when(linkEventsInEventLogDatabaseAccess.findIdOfNextEventToLink()).thenReturn(empty());

        assertThat(eventNumberLinker.findAndAndLinkNextUnlinkedEvent(), is(false));

        verifyNoMoreInteractions(linkEventsInEventLogDatabaseAccess);
        verifyNoMoreInteractions(eventNumberSequenceDataAccess);
    }
}