package uk.gov.justice.services.event.sourcing.subscription.catchup.consumer.task;

import static java.lang.String.format;

import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.eventsourcing.publishedevent.prepublish.MetadataEventNumberUpdater;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.MissingEventNumberException;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;

import javax.inject.Inject;
import javax.json.JsonObject;

public class LinkedEventMetadataUpdater {

    @Inject
    private MetadataEventNumberUpdater metadataEventNumberUpdater;

    @Inject
    private StringToJsonObjectConverter stringToJsonObjectConverter;

    @Inject
    private DefaultJsonEnvelopeProvider defaultJsonEnvelopeProvider;

    public LinkedEvent addEventNumbersToMetadataOf(final LinkedEvent linkedEvent) {

        final Long eventNumber = linkedEvent.getEventNumber().orElseThrow(
                () -> new MissingEventNumberException(format("Event with eventId '%s' has no event number", linkedEvent.getId()))
        );
        final Long previousEventNumber = linkedEvent.getPreviousEventNumber();

        final JsonObject metadataJsonObject = stringToJsonObjectConverter.convert(linkedEvent.getMetadata());
        final Metadata metadata = defaultJsonEnvelopeProvider.metadataFrom(metadataJsonObject).build();
        final Metadata metadataWithEventNumbers = metadataEventNumberUpdater.updateMetadataJson(
                metadata,
                previousEventNumber,
                eventNumber);

        final String metadataWithEventNumbersJson = metadataWithEventNumbers.asJsonObject().toString();

        return new LinkedEvent(
                linkedEvent.getId(),
                linkedEvent.getStreamId(),
                linkedEvent.getPositionInStream(),
                linkedEvent.getName(),
                metadataWithEventNumbersJson,
                linkedEvent.getPayload(),
                linkedEvent.getCreatedAt(),
                eventNumber,
                previousEventNumber
        );
    }
}
