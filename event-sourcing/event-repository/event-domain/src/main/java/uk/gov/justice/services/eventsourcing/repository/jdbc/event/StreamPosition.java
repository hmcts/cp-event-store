package uk.gov.justice.services.eventsourcing.repository.jdbc.event;

import java.util.UUID;

public record StreamPosition(UUID streamId, Long positionInStream) {
}