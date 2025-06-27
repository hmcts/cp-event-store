package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferEvent;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.NewEventBufferRepository;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPositionInStreamException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.eventsourcing.util.messaging.EventSourceNameCalculator;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
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

    @Mock
    private BufferedEventFactory bufferedEventFactory;

    @Mock
    private UtcClock clock;

    @Mock
    private EventSourceNameCalculator eventSourceNameCalculator;

    @InjectMocks
    private NewEventBufferManager newEventBufferManager;

    @Test
    public void shouldGetNextEventInEventBufferAndConvertToJsonEnvelope() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";
        final String jsonEnvelopeJson = "some-envelope-json";

        final long currentPosition = 23;
        final long nextPosition = currentPosition + 1;

        final JsonEnvelope currentJsonEnvelope = mock(JsonEnvelope.class);
        final JsonEnvelope nextJsonEnvelope = mock(JsonEnvelope.class);
        final EventBufferEvent eventBufferEvent = mock(EventBufferEvent.class);
        final Metadata metadata = mock(Metadata.class);

        when(currentJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(currentJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(currentPosition));
        when(newEventBufferRepository.findByPositionAndStream(streamId, nextPosition, source, componentName)).thenReturn(of(eventBufferEvent));
        when(eventBufferEvent.getEvent()).thenReturn(jsonEnvelopeJson);
        when(jsonObjectEnvelopeConverter.asEnvelope(jsonEnvelopeJson)).thenReturn(nextJsonEnvelope);

        final Optional<JsonEnvelope> nextFromEventBuffer = newEventBufferManager.getNextFromEventBuffer(currentJsonEnvelope, componentName);
        assertThat(nextFromEventBuffer, is(of(nextJsonEnvelope)));
    }

    @Test
    public void shouldReturnEmptyIfNoEventsFoundInEventBuffer() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component";

        final long currentPosition = 7263;
        final long nextPosition = currentPosition + 1;

        final JsonEnvelope currentJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(currentJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(currentJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(of(currentPosition));
        when(newEventBufferRepository.findByPositionAndStream(streamId, nextPosition, source, componentName)).thenReturn(empty());

        final Optional<JsonEnvelope> nextFromEventBuffer = newEventBufferManager.getNextFromEventBuffer(currentJsonEnvelope, componentName);
        assertThat(nextFromEventBuffer, is(empty()));
    }

    @Test
    public void shouldThrowMissingStreamIdExceptionIfNoStreamIdFoundInEventWhenGettingNextFromEventBuffer() throws Exception {

        final String eventName = "some-event-name";
        final String componentName = "some-component";
        final UUID eventId = fromString("b9d3f7a4-f6ee-4447-95bd-cf804fd2afc4");

        final JsonEnvelope currentJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(currentJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.streamId()).thenReturn(empty());
        when(metadata.id()).thenReturn(eventId);

        final MissingStreamIdException missingStreamIdException = assertThrows(
                MissingStreamIdException.class,
                () -> newEventBufferManager.getNextFromEventBuffer(currentJsonEnvelope, componentName));

        assertThat(missingStreamIdException.getMessage(), is("No streamId found in event. name 'some-event-name', eventId 'b9d3f7a4-f6ee-4447-95bd-cf804fd2afc4'"));
    }

    @Test
    public void shouldThrowMissingPositionInStreamExceptionIfNoPositionFoundInEventWhenGettingNextFromEventBuffer() throws Exception {

        final String eventName = "some-event-name";
        final String componentName = "some-component";
        final String source = "some-source";
        final UUID eventId = fromString("b9d3f7a4-f6ee-4447-95bd-cf804fd2afc4");
        final UUID streamId = randomUUID();

        final JsonEnvelope currentJsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(currentJsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.name()).thenReturn(eventName);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(eventSourceNameCalculator.getSource(currentJsonEnvelope)).thenReturn(source);
        when(metadata.position()).thenReturn(empty());

        final MissingPositionInStreamException missingPositionInStreamException = assertThrows(
                MissingPositionInStreamException.class,
                () -> newEventBufferManager.getNextFromEventBuffer(currentJsonEnvelope, componentName));

        assertThat(missingPositionInStreamException.getMessage(), is("No position found in event. name 'some-event-name', eventId 'b9d3f7a4-f6ee-4447-95bd-cf804fd2afc4'"));
    }

    @Test
    public void shouldBufferAnEventInTheEventBufferTable() throws Exception {

        final String componentName = "some-component";
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final JsonEnvelope incomingJsonEnvelope = mock(JsonEnvelope.class);
        final EventBufferEvent eventBufferEvent = mock(EventBufferEvent.class);

        when(clock.now()).thenReturn(bufferedAt);

        when(bufferedEventFactory.createFrom(
                incomingJsonEnvelope,
                componentName,
                bufferedAt)).thenReturn(eventBufferEvent);

        newEventBufferManager.addToBuffer(incomingJsonEnvelope, componentName);

        verify(newEventBufferRepository).insert(eventBufferEvent);
    }
}
