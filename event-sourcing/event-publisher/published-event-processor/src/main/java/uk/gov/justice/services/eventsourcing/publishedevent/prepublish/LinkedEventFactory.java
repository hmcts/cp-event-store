package uk.gov.justice.services.eventsourcing.publishedevent.prepublish;

import uk.gov.justice.services.eventsourcing.repository.jdbc.event.Event;
import uk.gov.justice.services.eventsourcing.repository.jdbc.event.LinkedEvent;
import uk.gov.justice.services.messaging.Metadata;

public class LinkedEventFactory {

    public LinkedEvent create(
            final Event event,
            final Metadata updatedMetadata,
            final long eventNumber,
            final Long previousEventNumber) {

        return new LinkedEvent(
                event.getId(),
                event.getStreamId(),
                event.getPositionInStream(),
                event.getName(),
                updatedMetadata.asJsonObject().toString(),
                event.getPayload(),
                event.getCreatedAt(),
                eventNumber,
                previousEventNumber);
    }
}
