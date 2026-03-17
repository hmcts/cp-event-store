package uk.gov.justice.services.eventsourcing.publishedevent.jdbc;

import java.util.UUID;

public record LinkedEventData(UUID eventId, long eventNumber, long previousEventNumber) {
}