package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferEvent;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.NewEventBufferRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingSourceException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.Metadata;

import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class NewEventBufferManagerTest {

    @Mock
    private NewEventBufferRepository newEventBufferRepository;

    @Mock
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @InjectMocks
    private NewEventBufferManager newEventBufferManager;

    @Test
    public void shouldFindNextEventInEventBufferAndConvertToJsonEnvelope() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final String jsonEnvelopeJson = "some-envelope-json";

        final JsonEnvelope currentJsonEnvelope = mock(JsonEnvelope.class);
        final JsonEnvelope nextJsonEnvelope = mock(JsonEnvelope.class);
        final EventBufferEvent eventBufferEvent = mock(EventBufferEvent.class);
        final Metadata metadata = mock(Metadata.class);

        when(currentJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(newEventBufferRepository.findNextForStream(streamId, source, componentName)).thenReturn(of(eventBufferEvent));
        when(eventBufferEvent.getEvent()).thenReturn(jsonEnvelopeJson);
        when(jsonObjectEnvelopeConverter.asEnvelope(jsonEnvelopeJson)).thenReturn(nextJsonEnvelope);

        final Optional<JsonEnvelope> nextFromEventBuffer = newEventBufferManager.getNextFromEventBuffer(currentJsonEnvelope, componentName);
        assertThat(nextFromEventBuffer, is(of(nextJsonEnvelope)));
    }

    @Test
    public void shouldReturnEmptyIfNoNextEventFoundInEventBuffer() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";

        final JsonEnvelope currentJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(currentJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(of(source));
        when(newEventBufferRepository.findNextForStream(streamId, source, componentName)).thenReturn(empty());

        final Optional<JsonEnvelope> nextFromEventBuffer = newEventBufferManager.getNextFromEventBuffer(currentJsonEnvelope, componentName);
        assertThat(nextFromEventBuffer, is(empty()));
    }

    @Test
    public void shouldThrowMissingStreamIdExceptionIfNoStreamIdPresentInIncomingEnvelope() throws Exception {

        final UUID eventId = fromString("2fb65e0d-79c6-435a-a64c-93dffffd7b15");
        final String eventName = "some-event-name";
        final String componentName = "some-component";

        final JsonEnvelope currentJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(currentJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.streamId()).thenReturn(empty());

        final MissingStreamIdException missingStreamIdException = assertThrows(
                MissingStreamIdException.class,
                () -> newEventBufferManager.getNextFromEventBuffer(currentJsonEnvelope, componentName));

        assertThat(missingStreamIdException.getMessage(), is("No streamId found in event. name 'some-event-name', eventId '2fb65e0d-79c6-435a-a64c-93dffffd7b15'"));
    }

    @Test
    public void shouldThrowMissingSourceExceptionIfNoStreamIdPresentInIncomingEnvelope() throws Exception {

        final UUID eventId = fromString("2fb65e0d-79c6-435a-a64c-93dffffd7b15");
        final UUID streamId = randomUUID();
        final String eventName = "some-event-name";
        final String componentName = "some-component";

        final JsonEnvelope currentJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(currentJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(empty());

        final MissingSourceException missingSourceException = assertThrows(
                MissingSourceException.class,
                () -> newEventBufferManager.getNextFromEventBuffer(currentJsonEnvelope, componentName));

        assertThat(missingSourceException.getMessage(), is("No source found in event. name 'some-event-name', eventId '2fb65e0d-79c6-435a-a64c-93dffffd7b15'"));
    }
}