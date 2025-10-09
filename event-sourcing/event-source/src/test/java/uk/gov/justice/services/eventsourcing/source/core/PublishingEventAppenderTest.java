package uk.gov.justice.services.eventsourcing.source.core;

import static co.unruly.matchers.OptionalMatchers.contains;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonObjects.jsonBuilderFactory;

import uk.gov.justice.services.eventsourcing.repository.jdbc.JdbcBasedEventRepository;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.StoreEventRequestFailedException;
import uk.gov.justice.services.eventsourcing.source.core.exception.EventStreamException;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class PublishingEventAppenderTest {

    private static final String DEFAULT_EVENT_SOURCE_NAME = "defaultEventSource";

    @Mock
    private JdbcBasedEventRepository eventRepository;

    @InjectMocks
    private PublishingEventAppender eventAppender;

    @Test
    public void shouldStoreEventInRepo() throws Exception {

        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();


        eventAppender.append(
                envelopeFrom(
                        metadataBuilder()
                                .withName("name123")
                                .withStreamId(eventId)
                                .withId(randomUUID()),
                        jsonBuilderFactory.createObjectBuilder()
                                .add("somePayloadField", "payloadValue123")
                ),
                streamId,
                3L, DEFAULT_EVENT_SOURCE_NAME);

        ArgumentCaptor<JsonEnvelope> envelopeCaptor = ArgumentCaptor.forClass(JsonEnvelope.class);

        verify(eventRepository).storeEvent(envelopeCaptor.capture());

        final JsonEnvelope storedEnvelope = envelopeCaptor.getValue();
        assertThat(storedEnvelope.metadata().streamId(), contains(streamId));
        assertThat(storedEnvelope.metadata().position(), contains(3L));
        assertThat(storedEnvelope.metadata().name(), is("name123"));
        assertThat(storedEnvelope.payloadAsJsonObject().getString("somePayloadField"), is("payloadValue123"));
    }

    @Test
    public void shouldThrowExceptionWhenStoreEventRequestFails() throws Exception {
        doThrow(StoreEventRequestFailedException.class).when(eventRepository).storeEvent(any());


        final JsonEnvelope jsonEnvelope = envelopeFrom(
                metadataBuilder()
                        .withName("name123")
                        .withId(randomUUID()),
                jsonBuilderFactory.createObjectBuilder()
                        .add("somePayloadField", "payloadValue123")
        );

        assertThrows(EventStreamException.class, () -> eventAppender.append(jsonEnvelope, randomUUID(), 1l, DEFAULT_EVENT_SOURCE_NAME));
    }

    @Test
    public void shouldStoreANewEventStream() throws EventStreamException {
        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();

        final long firstStreamEvent = 1L;
        eventAppender.append(
                envelopeFrom(
                        metadataBuilder()
                                .withName("name456")
                                .withStreamId(streamId)
                                .withId(eventId),
                        jsonBuilderFactory.createObjectBuilder()
                                .add("somePayloadField", "payloadValue456")
                ),
                streamId,
                firstStreamEvent, DEFAULT_EVENT_SOURCE_NAME);

        final ArgumentCaptor<UUID> streamIdCapture = ArgumentCaptor.forClass(UUID.class);

        verify(eventRepository).createEventStream(streamIdCapture.capture());

        final UUID streamIdActual = streamIdCapture.getValue();
        assertThat(streamIdActual, is(streamId));
    }

    @Test
    public void shouldNotStoreANewEventStream() throws EventStreamException {
        final UUID eventId = randomUUID();
        final UUID streamId = randomUUID();
        final long secondStreamEvent = 2L;

        eventAppender.append(
                envelopeFrom(
                        metadataBuilder()
                                .withName("name456")
                                .withStreamId(streamId)
                                .withId(eventId),
                        jsonBuilderFactory.createObjectBuilder()
                                .add("somePayloadField", "payloadValue456")
                ),
                streamId,
                secondStreamEvent, DEFAULT_EVENT_SOURCE_NAME);

        verify(eventRepository, times(0)).
                createEventStream(streamId);
    }
}
