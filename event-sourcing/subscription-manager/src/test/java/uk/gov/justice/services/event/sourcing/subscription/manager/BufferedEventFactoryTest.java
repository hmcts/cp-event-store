package uk.gov.justice.services.event.sourcing.subscription.manager;

import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.fromString;
import static java.util.UUID.randomUUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.event.buffer.core.repository.streambuffer.EventBufferEvent;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingPoisitionInStreamException;
import uk.gov.justice.services.event.sourcing.subscription.error.MissingSourceException;
import uk.gov.justice.services.eventsourcing.source.api.streams.MissingStreamIdException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.Metadata;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BufferedEventFactoryTest {

    @Mock
    private JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @InjectMocks
    private BufferedEventFactory bufferedEventFactory;

    @Test
    public void shouldCreateBufferedEventFromJsonEnvelope() throws Exception {

        final UUID streamId = randomUUID();
        final String source = "some-source";
        final String componentName = "some-component-name";
        final String jsonEnvelopeAsJsonString = "json-envelope-as-json-string";
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final long positionInStream = 23L;
        final Metadata metadata = mock(Metadata.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.position()).thenReturn(of(positionInStream));
        when(metadata.source()).thenReturn(of(source));
        when(jsonObjectEnvelopeConverter.asJsonString(jsonEnvelope)).thenReturn(jsonEnvelopeAsJsonString);

        final EventBufferEvent eventBufferEvent = bufferedEventFactory.createFrom(jsonEnvelope, componentName, bufferedAt);

        assertThat(eventBufferEvent.getEvent(), is(jsonEnvelopeAsJsonString));
        assertThat(eventBufferEvent.getStreamId(), is(streamId));
        assertThat(eventBufferEvent.getComponent(), is(componentName));
        assertThat(eventBufferEvent.getSource(), is(source));
        assertThat(eventBufferEvent.getPosition(), is(positionInStream));
        assertThat(eventBufferEvent.getBufferedAt(), is(bufferedAt));
    }

    @Test
    public void shouldThrowMissingStreamIdExceptionIfNoStreamIdFoundInJsonEnvelope() throws Exception {

        final UUID eventId = fromString("f2bb6730-1653-4913-83e5-24155adb9280");
        final String name = "event-name";
        final String componentName = "some-component-name";
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.name()).thenReturn(name);
        when(metadata.streamId()).thenReturn(empty());

        final MissingStreamIdException missingStreamIdException = assertThrows(
                MissingStreamIdException.class,
                () -> bufferedEventFactory.createFrom(jsonEnvelope, componentName, bufferedAt));

        assertThat(missingStreamIdException.getMessage(), is("No streamId found in event: name 'event-name', eventId 'f2bb6730-1653-4913-83e5-24155adb9280'"));
    }

    @Test
    public void shouldThrowMissingSourceExceptionIfNoStreamIdFoundInJsonEnvelope() throws Exception {

        final UUID eventId = fromString("6418d3c2-96e4-410e-81a2-aa3c1f82e9f2");
        final UUID streamId = randomUUID();
        final String name = "event-name";
        final String componentName = "some-component-name";
        final String jsonEnvelopeAsJsonString = "json-envelope-as-json-string";
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final long positionInStream = 23L;
        final Metadata metadata = mock(Metadata.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.name()).thenReturn(name);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.source()).thenReturn(empty());

        final MissingSourceException missingSourceException = assertThrows(
                MissingSourceException.class,
                () -> bufferedEventFactory.createFrom(jsonEnvelope, componentName, bufferedAt));

        assertThat(missingSourceException.getMessage(), is("No source found in event: name 'event-name', eventId '6418d3c2-96e4-410e-81a2-aa3c1f82e9f2'"));
    }

    @Test
    public void shouldThrowMissingPositionInStreamExceptionIfNoStreamIdFoundInJsonEnvelope() throws Exception {

        final UUID eventId = fromString("008eabd8-75b8-445b-8a8a-4fd218448e8a");
        final UUID streamId = randomUUID();
        final String name = "event-name";
        final String source = "some-source";
        final String componentName = "some-component-name";
        final String jsonEnvelopeAsJsonString = "json-envelope-as-json-string";
        final ZonedDateTime bufferedAt = new UtcClock().now();

        final JsonEnvelope jsonEnvelope = mock(JsonEnvelope.class);
        final Metadata metadata = mock(Metadata.class);

        when(jsonEnvelope.metadata()).thenReturn(metadata);
        when(metadata.id()).thenReturn(eventId);
        when(metadata.name()).thenReturn(name);
        when(metadata.streamId()).thenReturn(of(streamId));
        when(metadata.position()).thenReturn(empty());
        when(metadata.source()).thenReturn(of(source));

        final MissingPoisitionInStreamException missingPoisitionInStreamException = assertThrows(
                MissingPoisitionInStreamException.class,
                () -> bufferedEventFactory.createFrom(jsonEnvelope, componentName, bufferedAt));

        assertThat(missingPoisitionInStreamException.getMessage(), is("No positionInStream found in event: name 'event-name', eventId '008eabd8-75b8-445b-8a8a-4fd218448e8a'"));
    }
}