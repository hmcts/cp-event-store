package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.eventsourcing.repository.jdbc.exception.InvalidStreamIdException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.JsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.JsonObject;

/**
 * Converter class to convert between {@link JsonEnvelope} and {@link Event}
 */
@ApplicationScoped
public class EventConverter {

    @Inject
    JsonObjectEnvelopeConverter jsonObjectEnvelopeConverter;

    @Inject
    StringToJsonObjectConverter stringToJsonObjectConverter;

    @Inject
    DefaultJsonEnvelopeProvider defaultJsonEnvelopeProvider;


    /**
     * Creates an {@link Event} object from the <code>eventEnvelope</code>.
     *
     * @param envelope the envelope to convert from
     * @return the database entity created from the given envelope
     */
    public Event eventOf(final JsonEnvelope envelope) {

        final Metadata eventMetadata = envelope.metadata();

        return new Event(
                eventMetadata.id(),
                eventMetadata.streamId().orElseThrow(() -> new InvalidStreamIdException("StreamId missing in envelope.")),
                eventMetadata.position().orElse(null),
                eventMetadata.name(),
                envelope.metadata().asJsonObject().toString(),
                extractPayloadAsString(envelope),
                eventMetadata.createdAt().orElseThrow(() -> new IllegalArgumentException("createdAt field missing in envelope")),
                eventMetadata.eventNumber()
        );
    }

    /**
     * Creates an {@link JsonEnvelope} from {@link Event}
     *
     * @param event event to be converted into an envelope.
     * @return an envelope created from event.
     */
    public JsonEnvelope envelopeOf(final Event event) {
        return defaultJsonEnvelopeProvider.envelopeFrom(metadataOf(event), payloadOf(event));
    }

    /**
     * Retrieves metadata from event.
     *
     * @param event event containing the metadata.
     * @return metadata from the event.
     */
    public Metadata metadataOf(final Event event) {
        final JsonObject jsonObject = stringToJsonObjectConverter.convert(event.getMetadata());
        return defaultJsonEnvelopeProvider.metadataFrom(jsonObject).build();
    }

    private JsonObject payloadOf(final Event event) {
        return stringToJsonObjectConverter.convert(event.getPayload());
    }

    private String extractPayloadAsString(final JsonEnvelope envelope) {
        return jsonObjectEnvelopeConverter.extractPayloadFromEnvelope(
                jsonObjectEnvelopeConverter.fromEnvelope(envelope)).toString();
    }

}
