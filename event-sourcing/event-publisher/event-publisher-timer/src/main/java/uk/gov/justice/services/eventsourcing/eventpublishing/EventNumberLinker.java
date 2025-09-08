package uk.gov.justice.services.eventsourcing.eventpublishing;

import static javax.transaction.Transactional.TxType.REQUIRES_NEW;

import uk.gov.justice.services.common.converter.StringToJsonObjectConverter;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkEventsInEventLogDatabaseAccess;
import uk.gov.justice.services.eventsourcing.publishedevent.jdbc.LinkableEventDetails;
import uk.gov.justice.services.eventsourcing.publishedevent.prepublish.MetadataEventNumberUpdater;
import uk.gov.justice.services.messaging.Metadata;
import uk.gov.justice.services.messaging.spi.DefaultJsonEnvelopeProvider;

import java.util.Optional;
import java.util.UUID;

import javax.inject.Inject;
import javax.json.JsonObject;
import javax.transaction.Transactional;

public class EventNumberLinker {

    @Inject
    private LinkEventsInEventLogDatabaseAccess linkEventsInEventLogDatabaseAccess;

    @Inject
    private MetadataEventNumberUpdater metadataEventNumberUpdater;

    @Inject
    private DefaultJsonEnvelopeProvider defaultJsonEnvelopeProvider;

    @Inject
    private StringToJsonObjectConverter stringToJsonObjectConverter;

    @Transactional(REQUIRES_NEW)
    public boolean findAndAndLinkNextUnlinkedEvent() {

        final Optional<LinkableEventDetails> nextUnlinkedEvent =
                linkEventsInEventLogDatabaseAccess.findNextUnlinkedEvent();

        if (nextUnlinkedEvent.isPresent()) {
            final LinkableEventDetails linkableEventDetails = nextUnlinkedEvent.get();

            final Long eventNumber = linkableEventDetails.eventNumber();
            final UUID eventId = linkableEventDetails.eventId();
            final Long previousEventNumber = linkEventsInEventLogDatabaseAccess.findNextUnlinkedPreviousEventNumber();

            final JsonObject metadataJsonObject = stringToJsonObjectConverter.convert(linkableEventDetails.metadata());
            final Metadata metadata = defaultJsonEnvelopeProvider.metadataFrom(metadataJsonObject).build();
            final Metadata updatedMetadata = metadataEventNumberUpdater.updateMetadataJson(
                    metadata,
                    previousEventNumber,
                    eventNumber);

            final String updatedMetadataJson = updatedMetadata.asJsonObject().toString();

            linkEventsInEventLogDatabaseAccess.linkEventInEventLogTable(
                    eventId,
                    previousEventNumber,
                    updatedMetadataJson);

            linkEventsInEventLogDatabaseAccess.insertLinkedEventIntoPublishQueue(eventId);
        }

        return nextUnlinkedEvent.isPresent();
    }
}
