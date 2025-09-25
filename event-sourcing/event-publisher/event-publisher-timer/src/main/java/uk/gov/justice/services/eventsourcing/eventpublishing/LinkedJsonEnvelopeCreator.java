package uk.gov.justice.services.eventsourcing.eventpublishing;

import static java.lang.String.format;

import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.eventsourcing.publishedevent.prepublish.MetadataEventNumberUpdater;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.messaging.JsonEnvelope;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;

import java.util.Optional;

import javax.inject.Inject;
import javax.json.JsonObject;

public class LinkedJsonEnvelopeCreator {

    @Inject
    private MetadataEventNumberUpdater newMetadataEventNumberUpdater;

    @Inject
    private DefaultJsonEnvelopeProvider defaultJsonEnvelopeProvider;

    @Inject
    private StringToJsonObjectConverter stringToJsonObjectConverter;

    public JsonEnvelope createLinkedJsonEnvelopeFrom(final LinkedEvent linkedEvent) {

        final String metadataJson = linkedEvent.getMetadata();
        final Optional<Long> eventNumber = linkedEvent.getEventNumber();
        final Long previousEventNumber = linkedEvent.getPreviousEventNumber();

        final JsonObject metadataJsonObject = stringToJsonObjectConverter.convert(metadataJson);
        final Metadata metadata = defaultJsonEnvelopeProvider.metadataFrom(metadataJsonObject).build();

        final Metadata updatedMetadata = newMetadataEventNumberUpdater.updateMetadataJson(
                metadata,
                previousEventNumber,
                eventNumber.orElseThrow(() -> new MissingEventNumberException(format("Linked event with eventId '%s' from event_log table has null event_number", linkedEvent.getId()))));

        final JsonObject payload = stringToJsonObjectConverter.convert(linkedEvent.getPayload());

        return defaultJsonEnvelopeProvider.envelopeFrom(updatedMetadata, payload);
    }
}
