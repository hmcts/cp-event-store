package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import java.util.UUID;

public record EventDetailsToLink(UUID eventId, UUID streamId, long positionInStream) {

}