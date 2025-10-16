package uk.gov.justice.services.test.utils.events;

import static java.util.UUID.fromString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static uk.gov.justice.services.test.utils.events.LinkedEventBuilder.linkedEventBuilder;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;

import java.time.ZonedDateTime;
import java.util.UUID;

import org.junit.jupiter.api.Test;

public class LinkedEventBuilderTest {

    @Test
    public void shouldBuildPublishedEvent() throws Exception {

        final UUID eventId = fromString("ca530ced-b905-43b4-ab77-196bb2b8273b");
        final String eventName = "event-name";
        final UUID streamId = fromString("19a4ec31-26f6-470c-857b-a0779807a945");
        final long positionInStream = 23L;
        final String source = "EVENT_LISTENER";
        final ZonedDateTime timestamp = new UtcClock().now();
        final long previousEventNumber = 23;
        final long eventNumber = 24;

        final String expectedMetadata =
                "{\"stream\":{" +
                        "\"id\":\"19a4ec31-26f6-470c-857b-a0779807a945\"" +
                        "}," +
                        "\"name\":\"event-name\"," +
                        "\"id\":\"ca530ced-b905-43b4-ab77-196bb2b8273b\"," +
                        "\"source\":\"EVENT_LISTENER\"}";

        final LinkedEvent linkedEvent = linkedEventBuilder()
                .withId(eventId)
                .withName(eventName)
                .withStreamId(streamId)
                .withPositionInStream(positionInStream)
                .withSource(source)
                .withTimestamp(timestamp)
                .withEventNumber(eventNumber)
                .withPreviousEventNumber(previousEventNumber)
                .build();

        assertThat(linkedEvent.getId(), is(eventId));
        assertThat(linkedEvent.getStreamId(), is(streamId));
        assertThat(linkedEvent.getName(), is(eventName));
        assertThat(linkedEvent.getPositionInStream(), is(positionInStream));
        assertThat(linkedEvent.getCreatedAt(), is(timestamp));
        assertThat(linkedEvent.getEventNumber().orElse(-1L), is(eventNumber));
        assertThat(linkedEvent.getPreviousEventNumber(), is(previousEventNumber));

        assertThat(linkedEvent.getPayload(), is("{\"field_23\":\"value_23\"}"));
        assertThat(linkedEvent.getMetadata(), is(expectedMetadata));
    }

    @Test
    public void shouldUseMetadataAndPayloadIfProvided() throws Exception {

        final UUID eventId = fromString("ca530ced-b905-43b4-ab77-196bb2b8273b");
        final String eventName = "event-name";
        final UUID streamId = fromString("19a4ec31-26f6-470c-857b-a0779807a945");
        final long positionInStream = 23L;
        final String source = "EVENT_LISTENER";
        final ZonedDateTime timestamp = new UtcClock().now();
        final long previousEventNumber = 23;
        final long eventNumber = 24;

        final String payload = "payload";
        final String metadata = "metadata";

        final LinkedEvent linkedEvent = linkedEventBuilder()
                .withId(eventId)
                .withName(eventName)
                .withStreamId(streamId)
                .withPositionInStream(positionInStream)
                .withSource(source)
                .withTimestamp(timestamp)
                .withPreviousEventNumber(previousEventNumber)
                .withEventNumber(eventNumber)
                .withPayloadJSON(payload)
                .withMetadataJSON(metadata)
                .build();

        assertThat(linkedEvent.getId(), is(eventId));
        assertThat(linkedEvent.getStreamId(), is(streamId));
        assertThat(linkedEvent.getName(), is(eventName));
        assertThat(linkedEvent.getPositionInStream(), is(positionInStream));
        assertThat(linkedEvent.getCreatedAt(), is(timestamp));
        assertThat(linkedEvent.getEventNumber().orElse(-1L), is(eventNumber));
        assertThat(linkedEvent.getPreviousEventNumber(), is(previousEventNumber));

        assertThat(linkedEvent.getPayload(), is(payload));
        assertThat(linkedEvent.getMetadata(), is(metadata));
    }}
