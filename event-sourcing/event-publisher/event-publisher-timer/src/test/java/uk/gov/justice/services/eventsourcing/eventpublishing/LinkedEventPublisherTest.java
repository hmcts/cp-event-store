package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.EventPublishingRepository;
import uk.gov.justice.services.eventsourcing.publisher.jms.EventPublisher;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.EventConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LinkedEventPublisherTest {

    @Mock
    private EventPublisher eventPublisher;

    @Mock
    private EventConverter eventConverter;

    @Mock
    private EventPublishingRepository eventPublishingRepository;

    @InjectMocks
    private LinkedEventPublisher linkedEventPublisher;

    @Test
    public void shouldGetNextEventIdFromPublishQueueFetchEventFromEventLogAndPublish() throws Exception {

        final UUID eventId = randomUUID();
        final PublishedEvent publishedEvent = mock(PublishedEvent.class);
        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);

        when(eventPublishingRepository.getNextEventIdFromPublishQueue()).thenReturn(of(eventId));
        when(eventPublishingRepository.findEventFromEventLog(eventId)).thenReturn(of(publishedEvent));
        when(eventConverter.envelopeOf(publishedEvent)).thenReturn(jsonEnvelope);

        assertThat(linkedEventPublisher.publishNextQueuedEvent(), is(true));

        verify(eventPublisher).publish(jsonEnvelope);
    }

    @Test
    public void shouldDoNothingIfNoEventIdsFoundInPublishQueue() throws Exception {

        when(eventPublishingRepository.getNextEventIdFromPublishQueue()).thenReturn(empty());

        assertThat(linkedEventPublisher.publishNextQueuedEvent(), is(false));

        verifyNoMoreInteractions(eventPublishingRepository);
        verifyNoInteractions(eventPublisher);
    }
}