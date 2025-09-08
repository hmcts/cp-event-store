package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import java.util.UUID;

public record LinkableEventDetails(UUID eventId, Long eventNumber, String metadata) {
}
