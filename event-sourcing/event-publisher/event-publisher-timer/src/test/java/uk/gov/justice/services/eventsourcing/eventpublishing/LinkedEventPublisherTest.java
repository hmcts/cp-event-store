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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.eventpublishing.configuration.EventPublishingWorkerConfig;
import uk.gov.justice.services.eventsourcing.publishedevent.EventPublishingException;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.CompatibilityModePublishedEventRepository;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventPublishingRepository;
import uk.gov.justice.services.eventsourcing.publisher.jms.EventPublisher;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
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

    @Mock
    private EventPublishingWorkerConfig eventPublishingWorkerConfig;

    @Mock
    private CompatibilityModePublishedEventRepository compatibilityModePublishedEventRepository;

    @InjectMocks
    private LinkedEventPublisher linkedEventPublisher;

    @Test
    public void shouldGetEventIdFromPublishQueueFindTheEventInEventLogAddPreviousAndNextEventNumberToMetadataAndPublish() throws Exception {
        final UUID eventId = randomUUID();
        final Long eventNumber = 23L;

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope linkedJsonEnvelope = mock(JsonEnvelope.class);

        when(eventPublishingRepository.popNextEventIdFromPublishQueue()).thenReturn(of(eventId));
        when(eventPublishingRepository.findEventFromEventLog(eventId)).thenReturn(of(linkedEvent));
        when(linkedJsonEnvelopeCreator.createLinkedJsonEnvelopeFrom(linkedEvent)).thenReturn(linkedJsonEnvelope);
        when(eventPublishingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable()).thenReturn(true);
        when(linkedEvent.getEventNumber()).thenReturn(of(eventNumber));

        assertThat(linkedEventPublisher.publishNextNewEvent(), is(true));

        final InOrder inOrder = inOrder(
                eventPublisher,
                eventPublishingRepository,
                compatibilityModePublishedEventRepository);

        inOrder.verify(eventPublisher).publish(linkedJsonEnvelope);
        inOrder.verify(eventPublishingRepository).setIsPublishedFlag(eventId, true);
        inOrder.verify(compatibilityModePublishedEventRepository).insertIntoPublishedEvent(linkedJsonEnvelope);
        inOrder.verify(compatibilityModePublishedEventRepository).setEventNumberSequenceTo(eventNumber);
    }

    @Test
    public void shouldNotPublishEventToPublishedEventTableUnlessCompatibilityModeIsOn() throws Exception {
        final UUID eventId = randomUUID();

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope linkedJsonEnvelope = mock(JsonEnvelope.class);

        when(eventPublishingRepository.popNextEventIdFromPublishQueue()).thenReturn(of(eventId));
        when(eventPublishingRepository.findEventFromEventLog(eventId)).thenReturn(of(linkedEvent));
        when(linkedJsonEnvelopeCreator.createLinkedJsonEnvelopeFrom(linkedEvent)).thenReturn(linkedJsonEnvelope);
        when(eventPublishingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable()).thenReturn(false);

        assertThat(linkedEventPublisher.publishNextNewEvent(), is(true));

        final InOrder inOrder = inOrder(
                eventPublisher,
                eventPublishingRepository,
                compatibilityModePublishedEventRepository);

        inOrder.verify(eventPublisher).publish(linkedJsonEnvelope);
        inOrder.verify(eventPublishingRepository).setIsPublishedFlag(eventId, true);
        inOrder.verify(compatibilityModePublishedEventRepository, never()).insertIntoPublishedEvent(linkedJsonEnvelope);
    }

    @Test
    public void shouldThrowMissingEventNumberExceptionIfEventInEvenLongTableHasNullEventNumber() throws Exception {
        final UUID eventId = fromString("15892431-6cb7-43d4-9d55-1aef19a6d087");

        final LinkedEvent linkedEvent = mock(LinkedEvent.class);
        final JsonEnvelope linkedJsonEnvelope = mock(JsonEnvelope.class);

        when(eventPublishingRepository.popNextEventIdFromPublishQueue()).thenReturn(of(eventId));
        when(eventPublishingRepository.findEventFromEventLog(eventId)).thenReturn(of(linkedEvent));
        when(linkedJsonEnvelopeCreator.createLinkedJsonEnvelopeFrom(linkedEvent)).thenReturn(linkedJsonEnvelope);
        when(eventPublishingWorkerConfig.shouldAlsoInsertEventIntoPublishedEventTable()).thenReturn(true);
        when(linkedEvent.getEventNumber()).thenReturn(empty());

        final MissingEventNumberException missingEventNumberException = assertThrows(
                MissingEventNumberException.class,
                () -> linkedEventPublisher.publishNextNewEvent());

        assertThat(missingEventNumberException.getMessage(), is("Event with id '15892431-6cb7-43d4-9d55-1aef19a6d087' has null event_number in event_log table"));

    }

    @Test
    public void shouldDoNothingIfNoEventIdsFoundInPublishQueue() throws Exception {

        when(eventPublishingRepository.popNextEventIdFromPublishQueue()).thenReturn(empty());

        assertThat(linkedEventPublisher.publishNextNewEvent(), is(false));

        verifyNoMoreInteractions(eventPublishingRepository);
        verifyNoInteractions(eventPublisher);
        verifyNoInteractions(compatibilityModePublishedEventRepository);
    }

    @Test
    public void shouldThrowEventPublishingExceptionIfEventIdFoundInPublishQueueButNoEventExistsInEventLog() throws Exception {

        final UUID eventId = fromString("933248cd-a5d4-417c-b28c-709ab009ab50");

        when(eventPublishingRepository.popNextEventIdFromPublishQueue()).thenReturn(of(eventId));
        when(eventPublishingRepository.findEventFromEventLog(eventId)).thenReturn(empty());

        final EventPublishingException eventPublishingException = assertThrows(
                EventPublishingException.class,
                () -> linkedEventPublisher.publishNextNewEvent());

        assertThat(eventPublishingException.getMessage(), is("Failed to find LinkedEvent in event_log with id '933248cd-a5d4-417c-b28c-709ab009ab50' when id exists in publish_queue table"));

        verify(eventPublishingRepository, never()).setIsPublishedFlag(eventId, true);
        verifyNoInteractions(compatibilityModePublishedEventRepository);
    }
}