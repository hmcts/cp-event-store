package uk.gov.justice.services.test.utils.events;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.UUID.randomUUID;
import static uk.gov.justice.services.messaging.JsonEnvelope.envelopeFrom;
import static uk.gov.justice.services.messaging.JsonEnvelope.metadataBuilder;
import static uk.gov.justice.services.messaging.JsonObjects.getJsonBuilderFactory;

import uk.gov.justice.services.common.util.UtcClock;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.PublishedEvent;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.time.ZonedDateTime;
import java.util.UUID;

public class PublishedEventBuilder {

    private UUID id = randomUUID();
    private UUID streamId = randomUUID();
    private Long positionInStream = 5L;
    private String source = "EVENT_LISTENER";
    private String name = "Test Name";
    private String metadataJSON;
    private String payloadJSON;
    private ZonedDateTime timestamp = new UtcClock().now().truncatedTo(MILLIS);
    private Long eventNumber = 23L;
    private Long previousEventNumber = 22L;

    protected PublishedEventBuilder() {}

    public static PublishedEventBuilder publishedEventBuilder() {
        return new PublishedEventBuilder();
    }

    public PublishedEventBuilder withId(final UUID id) {
        this.id = id;
        return this;
    }

    public PublishedEventBuilder withSource(final String source) {
        this.source = source;
        return this;
    }

    public PublishedEventBuilder withStreamId(final UUID streamId) {
        this.streamId = streamId;
        return this;
    }

    public PublishedEventBuilder withPositionInStream(final Long positionInStream) {
        this.positionInStream = positionInStream;
        return this;
    }

    public PublishedEventBuilder withName(final String name) {
        this.name = name;
        return this;
    }

    public PublishedEventBuilder withMetadataJSON(final String metadataJSON) {
        this.metadataJSON = metadataJSON;
        return this;
    }

    public PublishedEventBuilder withPayloadJSON(final String payloadJSON) {
        this.payloadJSON = payloadJSON;
        return this;
    }

    public PublishedEventBuilder withTimestamp(final ZonedDateTime timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public PublishedEventBuilder withEventNumber(final long eventNumber) {
        this.eventNumber = eventNumber;
        return this;
    }

    public PublishedEventBuilder withPreviousEventNumber(final long previousEventNumber) {
        this.previousEventNumber = previousEventNumber;
        return this;
    }

    public PublishedEvent build() {

        final JsonEnvelope envelope = envelopeFrom(
                metadataBuilder()
                        .withId(id)
                        .withName(name)
                        .withStreamId(streamId)
                        .withSource(source),
                getJsonBuilderFactory().createObjectBuilder()
                        .add("field_" + positionInStream, "value_" + positionInStream));

        if (metadataJSON == null) {
            metadataJSON = envelope.metadata().asJsonObject().toString();
        }

        if (payloadJSON == null) {
            payloadJSON = envelope.payload().toString();
        }

        return new PublishedEvent(id, streamId, positionInStream, name, metadataJSON, payloadJSON, timestamp, eventNumber, previousEventNumber);
    }
}
