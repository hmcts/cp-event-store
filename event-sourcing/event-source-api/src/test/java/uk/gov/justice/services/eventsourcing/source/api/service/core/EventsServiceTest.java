package uk.gov.justice.services.eventsourcing.source.api.service.core;

import static java.time.ZonedDateTime.now;
import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.services.eventsourcing.source.api.service.core.Direction.BACKWARD;
import static uk.gov.justice.services.eventsourcing.source.api.service.core.Direction.FORWARD;
import static uk.gov.justice.services.eventsourcing.source.api.service.core.Position.first;
import static uk.gov.justice.services.eventsourcing.source.api.service.core.Position.head;
import static uk.gov.justice.services.eventsourcing.source.api.service.core.Position.position;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonObjects.jsonBuilderFactory;

import uk.gov.justice.services.common.converter.ZonedDateTimes;
import uk.gov.justice.services.eventsourcing.source.core.EventSource;
import uk.gov.justice.services.eventsourcing.source.core.EventStream;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import javax.json.JsonObject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;


@ExtendWith(MockitoExtension.class)
public class EventsServiceTest {

    @Mock
    private EventSource eventSource;

    @InjectMocks
    private EventsService service;

    @Test
    public void shouldReturnHeadEvents() throws Exception {

        final UUID streamId = randomUUID();
        final UUID firstEventId = randomUUID();
        final UUID secondEventId = randomUUID();
        final ZonedDateTime event1CreatedAt = now();
        final ZonedDateTime event2CreatedAt = now();
        final long pageSize = 2L;

        final JsonObject payload1 = jsonBuilderFactory.createObjectBuilder().add("field1", "value1").build();
        final JsonObject payload2 = jsonBuilderFactory.createObjectBuilder().add("field2", "value2").build();

        final JsonEnvelope event1 = envelopeFrom(
                metadataBuilder()
                        .withId(firstEventId)
                        .withName("Test Name1")
                        .withVersion(1L)
                        .withStreamId(streamId)
                        .createdAt(event1CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field1", "value1")
        );

        final JsonEnvelope event2 = envelopeFrom(
                metadataBuilder()
                        .withId(secondEventId)
                        .withName("Test Name2")
                        .withVersion(2L)
                        .withStreamId(streamId)
                        .createdAt(event2CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field2", "value2")
        );

        final EventStream eventStream = mock(EventStream.class);

        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        when(eventStream.readFrom(eventStream.size() - 1)).thenReturn(Stream.of(event1, event2));

        final List<EventEntry> entries = service.events(streamId, head(), BACKWARD, pageSize);

        assertThat(entries, hasSize(2));

        assertThat(entries.get(0).getStreamId(), is(streamId.toString()));
        assertThat(entries.get(0).getName(), is("Test Name1"));
        assertThat(entries.get(0).getPosition(), is(1L));
        assertThat(entries.get(0).getPayload(), is(payload1));
        assertThat(entries.get(0).getCreatedAt(), is(ZonedDateTimes.toString(event1CreatedAt)));

        assertThat(entries.get(1).getStreamId(), is(streamId.toString()));
        assertThat(entries.get(1).getName(), is("Test Name2"));
        assertThat(entries.get(1).getPosition(), is(2L));
        assertThat(entries.get(1).getCreatedAt(), is(ZonedDateTimes.toString(event2CreatedAt)));
        assertThat(entries.get(1).getPayload(), is(payload2));
    }

    @Test
    public void shouldReturnFirstEvents() throws Exception {

        final UUID streamId = randomUUID();
        final ZonedDateTime event1CreatedAt = now();
        final ZonedDateTime event2CreatedAt = now();
        final long pageSize = 2L;

        final JsonObject payload1 = jsonBuilderFactory.createObjectBuilder().add("field1", "value1").build();
        final JsonObject payload2 = jsonBuilderFactory.createObjectBuilder().add("field2", "value2").build();


        final JsonEnvelope event1 = envelopeFrom(
                metadataBuilder()
                        .withId(streamId)
                        .withName("Test Name1")
                        .withVersion(1L)
                        .withStreamId(streamId)
                        .createdAt(event1CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field1", "value1")
        );

        final JsonEnvelope event2 = envelopeFrom(
                metadataBuilder()
                        .withId(streamId)
                        .withName("Test Name2")
                        .withVersion(2L)
                        .withStreamId(streamId)
                        .createdAt(event2CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field2", "value2")
        );

        final EventStream eventStream = mock(EventStream.class);

        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        when(eventStream.readFrom(first().getPosition())).thenReturn(Stream.of(event1, event2));

        final List<EventEntry> eventEntries = service.events(streamId, first(), FORWARD, pageSize);

        assertThat(eventEntries, hasSize(2));

        assertThat(eventEntries.get(0).getStreamId(), is(streamId.toString()));
        assertThat(eventEntries.get(0).getName(), is("Test Name1"));
        assertThat(eventEntries.get(0).getPosition(), is(1L));
        assertThat(eventEntries.get(0).getPayload(), is(payload1));
        assertThat(eventEntries.get(0).getCreatedAt(), is(ZonedDateTimes.toString(event1CreatedAt)));

        assertThat(eventEntries.get(1).getStreamId(), is(streamId.toString()));
        assertThat(eventEntries.get(1).getName(), is("Test Name2"));
        assertThat(eventEntries.get(1).getPosition(), is(2L));
        assertThat(eventEntries.get(1).getCreatedAt(), is(ZonedDateTimes.toString(event2CreatedAt)));
        assertThat(eventEntries.get(1).getPayload(), is(payload2));
    }

    @Test
    public void shouldReturnPreviousEvents() throws Exception {

        final UUID streamId = randomUUID();
        final UUID firstEventId = randomUUID();
        final UUID secondEventId = randomUUID();
        final ZonedDateTime event2CreatedAt = now();
        final ZonedDateTime event3CreatedAt = now();
        final ZonedDateTime event4CreatedAt = now();
        final long pageSize = 2L;

        final JsonObject payload3 = jsonBuilderFactory.createObjectBuilder().add("field3", "value3").build();
        final JsonObject payload2 = jsonBuilderFactory.createObjectBuilder().add("field2", "value2").build();

        final JsonEnvelope event2 = envelopeFrom(
                metadataBuilder()
                        .withId(streamId)
                        .withName("Test Name2")
                        .withVersion(2L)
                        .withStreamId(streamId)
                        .createdAt(event2CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field2", "value2")
        );

        final JsonEnvelope event3 = envelopeFrom(
                metadataBuilder()
                        .withId(streamId)
                        .withName("Test Name3")
                        .withVersion(3L)
                        .withStreamId(streamId)
                        .createdAt(event3CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field3", "value3")
        );

        final JsonEnvelope event4 = envelopeFrom(
                metadataBuilder()
                        .withId(streamId)
                        .withName("Test Name4")
                        .withVersion(4L)
                        .withStreamId(streamId)
                        .createdAt(event4CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field4", "value4")
        );


        final EventStream eventStream = mock(EventStream.class);

        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);

        when(eventStream.readFrom(2L)).thenReturn(Stream.of(event2, event3, event4));

        final List<EventEntry> eventEntries = service.events(streamId, position(3L), BACKWARD, pageSize);

        assertThat(eventEntries, hasSize(2));

        assertThat(eventEntries.get(0).getStreamId(), is(streamId.toString()));
        assertThat(eventEntries.get(0).getName(), is("Test Name2"));
        assertThat(eventEntries.get(0).getPosition(), is(2L));
        assertThat(eventEntries.get(0).getPayload(), is(payload2));
        assertThat(eventEntries.get(0).getCreatedAt(), is(ZonedDateTimes.toString(event2CreatedAt)));

        assertThat(eventEntries.get(1).getStreamId(), is(streamId.toString()));
        assertThat(eventEntries.get(1).getName(), is("Test Name3"));
        assertThat(eventEntries.get(1).getPosition(), is(3L));
        assertThat(eventEntries.get(1).getCreatedAt(), is(ZonedDateTimes.toString(event3CreatedAt)));
        assertThat(eventEntries.get(1).getPayload(), is(payload3));
    }

    @Test
    public void shouldReturnEmptyList() {
        final UUID streamId = randomUUID();
        final Position position = Position.empty();
        final List<EventEntry> eventEntries = service.events(streamId, position, null, 1);

        assertThat(eventEntries, is(empty()));
    }


    @Test
    public void shouldReturnNextEvents() throws Exception {

        final UUID streamId = randomUUID();
        final UUID firstEventId = randomUUID();
        final UUID secondEventId = randomUUID();
        final ZonedDateTime event3CreatedAt = now();
        final ZonedDateTime event4CreatedAt = now();

        final long pageSize = 2L;

        final JsonObject payload4 = jsonBuilderFactory.createObjectBuilder().add("field4", "value4").build();
        final JsonObject payload3 = jsonBuilderFactory.createObjectBuilder().add("field3", "value3").build();

        final JsonEnvelope event4 = envelopeFrom(
                metadataBuilder()
                        .withId(secondEventId)
                        .withName("Test Name4")
                        .withVersion(4L)
                        .withStreamId(streamId)
                        .createdAt(event4CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field4", "value4")
        );
        final JsonEnvelope event3 = envelopeFrom(
                metadataBuilder()
                        .withId(firstEventId)
                        .withName("Test Name3")
                        .withVersion(3L)
                        .withStreamId(streamId)
                        .createdAt(event3CreatedAt),
                jsonBuilderFactory.createObjectBuilder().add("field3", "value3")
        );

        final EventStream eventStream = mock(EventStream.class);

        final long positionId = 3L;

        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        when(eventStream.readFrom(positionId)).thenReturn(Stream.of(event3, event4));

        final List<EventEntry> eventEntries = service.events(streamId, position(positionId), FORWARD, pageSize);

        assertThat(eventEntries, hasSize(2));

        assertThat(eventEntries.get(0).getStreamId(), is(streamId.toString()));
        assertThat(eventEntries.get(0).getName(), is("Test Name3"));
        assertThat(eventEntries.get(0).getPosition(), is(3L));
        assertThat(eventEntries.get(0).getPayload(), is(payload3));
        assertThat(eventEntries.get(0).getCreatedAt(), is(ZonedDateTimes.toString(event3CreatedAt)));

        assertThat(eventEntries.get(1).getStreamId(), is(streamId.toString()));
        assertThat(eventEntries.get(1).getName(), is("Test Name4"));
        assertThat(eventEntries.get(1).getPosition(), is(4L));
        assertThat(eventEntries.get(1).getCreatedAt(), is(ZonedDateTimes.toString(event4CreatedAt)));
        assertThat(eventEntries.get(1).getPayload(), is(payload4));
    }

    @Test
    public void shouldReturnEventExist() {
        final UUID streamId = randomUUID();
        final long position = 1L;

        final ZonedDateTime createdAt = now();

        final JsonEnvelope event = envelopeFrom(
                metadataBuilder()
                        .withId(streamId)
                        .withName("Test Name1")
                        .withVersion(position)
                        .withStreamId(streamId)
                        .createdAt(createdAt),
                jsonBuilderFactory.createObjectBuilder().add("field1", "value1")
        );

        final EventStream eventStream = mock(EventStream.class);

        when(eventSource.getStreamById(streamId)).thenReturn(eventStream);
        when(eventStream.readFrom(1L)).thenReturn(Stream.of(event));

        assertTrue(service.eventExists(streamId, position));
        verify(eventSource).getStreamById(streamId);
        verify(eventStream).readFrom(position);
    }
}
