package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventPublishingRepository;
import uk.gov.justice.services.eventsourcing.publisher.jms.EventPublisher;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventPublisherTest {

    @Mock
    private EventPublisher eventPublisher;

    @Mock
    private EventPublishingRepository eventPublishingRepository;

    @Mock
    private LinkedJsonEnvelopeCreator linkedJsonEnvelopeCreator;

    @InjectMocks
    private LinkedEventPublisher linkedEventPublisher;

    @Test
    public void shouldGetEventIdFromPublishQueueFindTheEventInEventLogAddPreviousAndNextEventNumberToMetadataAndPublish() throws Exception {
        final UUID eventId = randomUUID();

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope linkedJsonEnvelope = mock(JsonEnvelope.class);

        when(eventPublishingRepository.getNextEventIdFromPublishQueue()).thenReturn(of(eventId));
        when(eventPublishingRepository.findEventFromEventLog(eventId)).thenReturn(of(linkedEvent));
        when(linkedJsonEnvelopeCreator.createLinkedJsonEnvelopeFrom(linkedEvent)).thenReturn(linkedJsonEnvelope);

        assertThat(linkedEventPublisher.publishNextNewEvent(), is(true));

        final InOrder inOrder = inOrder(eventPublisher, eventPublishingRepository);

        inOrder.verify(eventPublisher).publish(linkedJsonEnvelope);
        inOrder.verify(eventPublishingRepository).removeFromPublishQueue(eventId);
    }

    @Test
    public void shouldDoNothingIfNoEventIdsFoundInPublishQueue() throws Exception {

        when(eventPublishingRepository.getNextEventIdFromPublishQueue()).thenReturn(empty());

        assertThat(linkedEventPublisher.publishNextNewEvent(), is(false));

        verifyNoMoreInteractions(eventPublishingRepository);
        verifyNoInteractions(eventPublisher);
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfEventIdFoundInPublishQueueButNoEventExistsInEventLog() throws Exception {

        final UUID eventId = fromString("933248cd-a5d4-417c-b28c-709ab009ab50");

        when(eventPublishingRepository.getNextEventIdFromPublishQueue()).thenReturn(of(eventId));
        when(eventPublishingRepository.findEventFromEventLog(eventId)).thenReturn(empty());

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> linkedEventPublisher.publishNextNewEvent());

        assertThat(eventPublishingException.getMessage(), is("Failed to find LinkedEvent in event_log with id '933248cd-a5d4-417c-b28c-709ab009ab50' when id exists in publish_queue table"));
    }
}