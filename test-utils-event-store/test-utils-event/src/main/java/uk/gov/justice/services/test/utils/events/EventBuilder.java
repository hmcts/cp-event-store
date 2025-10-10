package uk.gov.justice.services.test.utils.events;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.UUID.randomUUID;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonObjects.jsonBuilderFactory;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;

public class EventBuilder {

    private UUID id = randomUUID();
    private UUID streamId = randomUUID();
    private String source = "EVENT_LISTENER";
    private Long positionInStream = 5L;
    private String name = "Test Name";
    private String metadataJSON;
    private String payloadJSON;
    private ZonedDateTime timestamp = new UtcClock().now().truncatedTo(MILLIS);
    private Optional<Long> eventNumber = empty();

    private EventBuilder() {}

    public static EventBuilder eventBuilder() {
        return new EventBuilder();
    }

    public EventBuilder withId(final UUID id) {
        this.id = id;
        return this;
    }

    public EventBuilder withStreamId(final UUID streamId) {
        this.streamId = streamId;
        return this;
    }

    public EventBuilder withSource(final String source) {
        this.source = source;
        return this;
    }

    public EventBuilder withPositionInStream(final Long positionInStream) {
        this.positionInStream = positionInStream;
        return this;
    }

    public EventBuilder withName(final String name) {
        this.name = name;
        return this;
    }

    public EventBuilder withMetadataJSON(final String metadataJSON) {
        this.metadataJSON = metadataJSON;
        return this;
    }

    public EventBuilder withPayloadJSON(final String payloadJSON) {
        this.payloadJSON = payloadJSON;
        return this;
    }

    public EventBuilder withTimestamp(final ZonedDateTime timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public EventBuilder withEventNumber(final long eventNumber) {
        this.eventNumber = of(eventNumber);
        return this;
    }

    public Event build() {

        final JsonEnvelope envelope = envelopeFrom(
                metadataBuilder()
                        .withId(id)
                        .withName(name)
                        .withStreamId(streamId)
                        .withSource(source),
                jsonBuilderFactory.createObjectBuilder()
                        .add("field_" + positionInStream, "value_" + positionInStream));

        if (metadataJSON == null) {
            metadataJSON = envelope.metadata().asJsonObject().toString();
        }

        if (payloadJSON == null) {
            payloadJSON = envelope.payload().toString();
        }

        return new Event(id, streamId, positionInStream, name, metadataJSON, payloadJSON, timestamp, eventNumber);
    }
}
