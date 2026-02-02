package uk.gov.justice.services.eventsourcing.repository.jdbc.discovery;

import java.util.UUID;

public record EventIdNumber(UUID id, long eventNumber) {
}
